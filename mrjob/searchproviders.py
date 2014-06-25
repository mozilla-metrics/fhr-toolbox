import healthreportutils
from datetime import date, datetime, timedelta
import os, shutil, csv
import sys, codecs
import traceback

import mrjob
from mrjob.job import MRJob
import tempfile

try:
    import simplejson as json
except ImportError:
    import json

def active_day(day):
    if day is None:
        return False
    return any(k != "org.mozilla.crashes.crashes" for k in day)

def date_back(start, days):
    """iter backwards from start for N days"""
    date = start
    for n in xrange(0, days):
        yield date - timedelta(days=n)

def start_date(dstr):
    """
    Start measuring a few days before the snapshot was taken to give clients
    time to upload.
    """
    snapshot = datetime.strptime(dstr, "%Y-%m-%d").date()
    startdate = snapshot - timedelta(days=4)
    return startdate

def eat_exceptions(func):
    def wrapper(job, k, v):
        try:
            for k1, v1 in func(job, k, v):
                yield (k1, v1)
        except:
            exc = traceback.format_exc()
            print >>sys.stderr, "Script exception: ", exc
    return wrapper

@eat_exceptions
@healthreportutils.FHRMapper()
def map(job, key, payload):
    channel = payload.channel.split("-")[0]
    if channel != "beta":
        return

    locale = payload.last.get("org.mozilla.appInfo.appinfo", {}).get("locale", "?")
    if locale != "en-US":
        return

    days = payload.get('data', {}).get('days', {})
    def get_day(d):
        dstr = d.strftime("%Y-%m-%d")
        return days.get(dstr, None)

    sd = start_date(job.options.start_date)

    total_days = 0
    last_search = "UNKNOWN"

    for d in date_back(sd, 42):
        day = get_day(d)
        if not active_day(day):
            continue
        total_days += 1
        if "org.mozilla.searches.counts" in day and last_search == "UNKNOWN":
            for k, v in day["org.mozilla.searches.counts"].iteritems():
                if k == "_v":
                    continue
                last_search = k.rsplit(".", 1)[0]
                break

    geo = payload.get("geoCountry", "?")
    isactive = total_days >= 6

    yield ((geo, isactive, last_search), 1)

def reduce(job, k, vlist):
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

        outpath = self.options.output_path
        if outpath is None:
            raise Exception("--output-path is required")

        # Do the big work
        super(AggJob, self).run_job()

        # Produce the separated output files
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

def output(fd, path):
    writer = csv.writer(open(path, "w"))
    for k, v in getresults(fd):
        out = k + [v]
        out = [unicode(s).encode("utf-8") for s in out]
        writer.writerow(out)

if __name__ == '__main__':
    AggJob.run()

