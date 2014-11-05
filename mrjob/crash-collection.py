"""
Collect crash stats.
"""

import healthreportutils
from datetime import date, datetime, timedelta
import os, shutil, csv
import sys, codecs
import traceback

import mrjob
from mrjob.job import MRJob
import tempfile

from collections import defaultdict

try:
    import simplejson as json
except ImportError:
    import json

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

def day_sessions(day):
    """returns (seconds, ticks) for a day"""
    seconds = 0
    ticks = 0
    sessions = day.get("org.mozilla.appSessions.previous", None)
    if sessions:
        seconds += sum(sessions.get("cleanTotalTime", []))
        seconds += sum(sessions.get("abortedTotalTime", []))
        ticks += sum(sessions.get("cleanActiveTicks", []))
        ticks += sum(sessions.get("abortedActiveTicks", []))
    return seconds, ticks

class CrashType(object):
    __slots__ = ('crashes', 'submitSuccess', 'submitFailure')

    def __init__(self):
        self.crashes = 0
        self.submitSuccess = 0
        self.submitFailure = 0

crashtypes = ("main-crash", "plugin-crash", "plugin-hang", "gmplugin-crash", "content-crash")

@logexceptions
@healthreportutils.FHRMapper()
def mapjob(job, key, payload):
    channel = payload.channel.split("-")[0]
    if channel not in main_channels:
        return

    os = payload.last.get("org.mozilla.appInfo.appinfo", {}).get("os", "?")

    days = payload.get('data', {}).get('days', {})
    def get_day(d):
        dstr = d.strftime("%Y-%m-%d")
        return days.get(dstr, None)

    sd = start_date(job.options.start_date)

    crashes = defaultdict(CrashType)
    seconds = 0
    ticks = 0
    daycount = 0

    for d in date_back(sd, 7):
        dstr = d.strftime("%Y-%m-%d")
        day = get_day(d)
        if active_day(day):
            yield (("daily-active", dstr, channel, os), 1)
            daycount += 1

        if not day:
            continue

        s, t = day_sessions(day)

        yield (("daily-seconds", dstr, channel, os), s)
        seconds += s

        yield (("daily-ticks", dstr, channel, os), t)
        ticks += t

        cdata = day.get("org.mozilla.crashes.crashes", None)
        if not cdata:
            continue
        if cdata.get("_v", 0) < 4:
            continue
        for type in crashtypes:
            c = cdata.get(type, 0)
            yield (("daily", dstr, type, channel, os), c)
            crashes[type].crashes += c

            c = cdata.get(type + "-submission-succeeded", 0)
            yield (("daily-submission-succeeded", dstr, type, channel, os), c)
            crashes[type].submitSuccess += c

            c = cdata.get(type + "-submission-failed", 0)
            yield (("daily-submission-failed", dstr, type, channel, os), c)
            crashes[type].submitFailure += c

    if not daycount:
        return

    totals = tuple(crashes[type].crashes for type in crashtypes)
    yield (("totals", channel, os) + totals, 1)

    yield (("daycount", channel, os), daycount)
    yield (("seconds", channel, os), seconds)
    yield (("ticks", channel, os), ticks)

    for type in crashtypes:
        yield (("crashes", channel, os, type, "crashes"), crashes[type].crashes)
        yield (("crashes", channel, os, type, "submitSuccess"), crashes[type].submitSuccess)
        yield (("crashes", channel, os, type, "submitFailure"), crashes[type].submitFailure)

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
            outpath = os.path.expanduser("~/fhr-crashdata-" + self.options.start_date)
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
