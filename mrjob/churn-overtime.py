"""
Analyze a range of dates for new and lost Firefox profiles.
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

DAYS_PER_WEEK = 7
TOTAL_DAYS = 168

# How many weeks must a user be gone to be considered "lost"?
LAG_WEEKS = 4

TOTAL_WEEKS = TOTAL_DAYS / DAYS_PER_WEEK

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
    Measure Sunday-Saturday, for no particularly good reason.
    """
    snapshot = datetime.strptime(dstr, "%Y-%m-%d").date()
    startdate = last_saturday(snapshot)
    return startdate

def date_back(start, days, skip=1):
    """iter backwards from start for N days"""
    date = start
    for n in xrange(0, days, skip):
        yield date - timedelta(days=n)

def active_day(day):
    if day is None:
        return False
    return any(k != "org.mozilla.crashes.crashes" for k in day)

@healthreportutils.FHRMapper()
def map(job, key, payload):
    pingDate = payload.get("thisPingDate", "unknown")
    channel = payload.channel.split("-")[0]
    if channel != "release":
        return

    days = payload.get('data', {}).get('days', {})
    def get_day(d):
        dstr = d.strftime("%Y-%m-%d")
        return days.get(dstr, None)

    sd = start_date(job.options.start_date)

    weeks = [] # newest to oldest

    for weekno in range(0, TOTAL_WEEKS):
        active = False
        week_end = sd - timedelta(days=DAYS_PER_WEEK * weekno)
        for d in date_back(week_end, 7):
            day = get_day(d)
            if active_day(day):
                active = True
        weeks.append(active)

    osname = payload.last.get("org.mozilla.sysinfo.sysinfo", {}).get("name", "?")
    locale = payload.last.get("org.mozilla.appInfo.appinfo", {}).get("locale", "?")
    geo = payload.get("geoCountry", "?")

    # Figure out when this user appeared/disappeared
    for weekno in range(LAG_WEEKS, TOTAL_WEEKS - LAG_WEEKS):
        if weeks[weekno]:
            week_end = sd - timedelta(days=DAYS_PER_WEEK * weekno)
            if not any(weeks[weekno+1:weekno+LAG_WEEKS+1]):
                yield (("gain", week_end.strftime("%Y-%m-%d"), osname, locale, geo), 1)
            if not any(weeks[weekno-LAG_WEEKS:weekno]):
                yield (("loss", week_end.strftime("%Y-%m-%d"), osname, locale, geo), 1)

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

        # Do the big work
        super(AggJob, self).run_job()

        # Produce the separated output files
        outpath = self.options.output_path
        if outpath is None:
            outpath = os.path.expanduser("~/fhr-churn-overtime-" + self.options.start_date)
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
