"""
Collect plugin stats.
"""

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

# How many days must a user be gone to be considered "lost"?
LOSS_DAYS = 7 * 6 # 42 days/one release cycle
TOTAL_DAYS = 180

def intorstr(v):
    try:
        return int(v)
    except ValueError:
        return v

def compareversions(v1, v2):
    v1 = map(intorstr, v1.split('.'))
    v2 = map(intorstr, v2.split('.'))
    return cmp(v1, v2)

main_channels = (
    'nightly',
    'aurora',
    'beta',
    'release'
)

def last_saturday(d):
    """Return the Saturday on or before the date."""
    # .weekday in python starts on 0=Monday
    return d - timedelta(days=(d.weekday() + 2) % 7)

def start_date(dstr):
    """
    Start measuring a few days before the snapshot was taken to give clients
    time to upload.
    """
    snapshot = datetime.strptime(dstr, "%Y-%m-%d").date()
    startdate = last_saturday(snapshot) - timedelta(days=7)
    return startdate

def date_back(start, days):
    """iter backwards from start for N days"""
    date = start
    for n in xrange(0, days):
        yield date - timedelta(days=n)

def active_day(day):
    if day is None:
        return False
    return any(k != "org.mozilla.crashes.crashes" for k in day)

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
def mapjob(job, key, payload):
    channel = payload.channel.split("-")[0]
    if channel not in main_channels:
        return

    days = payload.get('data', {}).get('days', {})
    def get_day(d):
        dstr = d.strftime("%Y-%m-%d")
        return days.get(dstr, None)

    sd = start_date(job.options.start_date)
    week_end = sd # sd is always a Saturday

    active_user = False

    for d in date_back(sd, LOSS_DAYS):
        day = get_day(d)
        if active_day(day):
            active_user = True
            break

    if not active_user:
        return

    # require the v1 plugin data
    plugins = payload.last.get("org.mozilla.addons.plugins", {})
    plugins_v = plugins.get("_v", "?")
    if plugins_v != 1:
        return

    os = payload.last.get("org.mozilla.appInfo.appinfo", {}).get("os", "?")
    yield (("totals", channel, os), 1)

    # Sometimes multiple versions of the same plugin are present. Don't double-
    # count.
    pluginmap = {} # name

    for pluginid, data in plugins.items():
        if pluginid == "_v":
            continue

        name = data.get("name", "?")
        if name in pluginmap:
            if compareversions(data.get("version", "?"),
                               pluginmap[name].get("version", "?")) > 0:
                pluginmap[name] = data
        else:
            pluginmap[name] = data

    for data in pluginmap.values():
        yield (("plugins", channel, os,
                data.get("name", "?"),
                data.get("version", "?"),
                data.get("blocklisted", "?"),
                data.get("disabled", "?"),
                data.get("clicktoplay", "?")), 1)

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
            outpath = os.path.expanduser("~/fhr-plugindata-" + self.options.start_date)
        output(self.stdout, outpath)

    def configure_options(self):
        super(AggJob, self).configure_options()

        self.add_passthrough_option('--output-path', help="Specify output path",
                                    default=None)
        self.add_passthrough_option('--start-date', help="Specify start date",
                                    default=None)

    def mapper(self, key, value):
        return mapjob(self, key, value)

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
