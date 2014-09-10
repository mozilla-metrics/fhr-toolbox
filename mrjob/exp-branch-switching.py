import healthreportutils
from datetime import date, datetime, timedelta
import os, shutil, csv
import sys, codecs
import traceback

import mrjob
from mrjob.job import MRJob
import tempfile

import simplejson as json

def start_date(dstr):
    """
    Start measuring a few days before the snapshot was taken to give clients
    time to upload.
    """
    snapshot = datetime.strptime(dstr, "%Y-%m-%d").date()
    return snapshot

def date_back(start, days):
    """iter backwards from start for N days"""
    date = start
    for n in xrange(0, days):
        yield date - timedelta(days=n)

def logexceptions(func):
    def wrapper(job, k, v):
        try:
            for k1, v1 in func(job, k, v):
                yield (k1, v1)
        except:
            exc = traceback.format_exc()
            print >>sys.stderr, "Script exception: ", exc
            yield ("exception", exc)
    return wrapper

@logexceptions
@healthreportutils.FHRMapper()
def map(job, key, payload):
    days = payload.get('data', {}).get('days', {})
    def get_day(d):
        dstr = d.strftime("%Y-%m-%d")
        return days.get(dstr, None)

    branches = []

    sd = start_date(job.options.start_date)
    for d in date_back(sd, 42):
        day = get_day(d)
        if day is None:
            continue
        experiment = day.get("org.mozilla.experiments.info", {}).get("lastActive", "-")
        if experiment != "experiment-branch-test-nightly@experiments.mozilla.org":
            continue
        branch = day.get("org.mozilla.experiments.info", {}).get("lastActiveBranch")
        if len(branches) == 0 or branches[-1] != branch:
            branches.append(branch)

    yield (("branches", tuple(branches)), 1)

def reduce(job, k, vlist):
    if k == "exception":
        print >> sys.stderr, "FOUND exception", vlist
        for v in vlist:
            yield (k, v)
    else:
        yield (k, sum(vlist))

class AggJob(MRJob):
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.SequenceFileAsTextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def run_job(self):
        self.stdout = tempfile.TemporaryFile()

        if self.options.start_date is None:
            raise Exception("--start-date is required")
        # validate the start date here
        start_date(self.options.start_date)

        # Do the big work
        super(AggJob, self).run_job()

        # Produce the separated output files
        outpath = self.options.output_path
        if outpath is None:
            outpath = os.path.expanduser("~/nightly-branch-switching-" + self.options.start_date)
        output(self.stdout, outpath)

    def configure_options(self):
        super(AggJob, self).configure_options()

        self.add_passthrough_option('--output-path', help="Specify output path",
                                    default=None)
        self.add_passthrough_option('--start-date', help="Specify start date",
                                    default=None)

    def mapper(self, key, value):
        return map(self, key, value)

    def reducer(self, key, vlist):
        return reduce(self, key, vlist)

    combiner = reducer

def getresults(fd):
    fd.seek(0)
    for line in fd:
        k, v = line.split("\t")
        yield json.loads(k), json.loads(v)

def unwrap(l, v):
    """
    Unwrap a value into a list. Dicts are added in their repr form.
    """
    if isinstance(v, (tuple, list)):
        for e in v:
            unwrap(l, e)
    elif isinstance(v, dict):
        l.append(repr(v))
    elif isinstance(v, unicode):
        l.append(v.encode("utf-8"))
    else:
        l.append(v)

def output(fd, path):
    try:
        shutil.rmtree(path)
    except OSError:
        pass
    os.mkdir(path)

    writers = {}
    errs = codecs.getwriter("utf-8")(open(os.path.join(path, "exceptions.txt"), "w"))
    for k, v in getresults(fd):
        if k == "exception":
            print >>errs, "==ERR=="
            print >>errs, v
            continue
        l = []
        unwrap(l, k)
        unwrap(l, v)
        fname = l.pop(0)
        if fname in writers:
            w = writers[fname]
        else:
            fd = open(os.path.join(path, fname + ".csv"), "w")
            w = csv.writer(fd)
            writers[fname] = w
        w.writerow(l)

if __name__ == '__main__':
    AggJob.run()
