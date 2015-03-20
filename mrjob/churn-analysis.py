"""
Analyze a historical week to understand Firefox churn.
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
LAG_DAYS = 49
CRITICAL_WEEKS = 9
TOTAL_DAYS = 180

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
            raise
    return wrapper

@logexceptions
@healthreportutils.FHRMapper()
def map(job, key, payload):
    pingDate = payload.get("thisPingDate", "unknown")
    channel = payload.channel.split("-")[0]
    if channel not in main_channels:
        return

    days = payload.get('data', {}).get('days', {})
    def get_day(d):
        dstr = d.strftime("%Y-%m-%d")
        return days.get(dstr, None)

    version = payload.get("geckoAppInfo", {}).get("version", "?")
    sd = start_date(job.options.start_date)

    # Was the user active at all in the 49 days prior to the snapshot
    recent_usage = 0
    for d in date_back(sd, LAG_DAYS):
        day = get_day(d)
        if active_day(day):
            recent_usage = 1
            break

    # For each of the "critical" 9 weeks, record both usage days and default
    # status.
    week_actives = []
    for weekno in xrange(0, CRITICAL_WEEKS):
        week_end = sd - timedelta(days=LAG_DAYS + 7 * weekno)

        active_days = 0
        default_browser = None

        for d in date_back(week_end, 7):
            day = get_day(d)
            if active_day(day):
                active_days += 1
                if default_browser is None:
                    default_browser = day.get("org.mozilla.appInfo.appinfo", {}).get("isDefaultBrowser", None)

        if default_browser is None:
            default_browser = "?"

        week_actives.append(active_days)
        week_actives.append(default_browser)

    prior_usage = 0
    for d in date_back(sd - timedelta(days=LAG_DAYS + 7 * CRITICAL_WEEKS),
                       180 - (LAG_DAYS + 7 * CRITICAL_WEEKS)):
        day = get_day(d)
        if active_day(day):
            prior_usage = True
            break

    osname = payload.last.get("org.mozilla.sysinfo.sysinfo", {}).get("name", "?")
    locale = payload.last.get("org.mozilla.appInfo.appinfo", {}).get("locale", "?")
    geo = payload.get("geoCountry", "?")

    yield ("result", [channel, osname, locale, geo, pingDate, recent_usage] + week_actives + [prior_usage])

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
            outpath = os.path.expanduser("~/fhr-churnanalysis-" + self.options.start_date + ".csv")
        output(self.stdout, outpath)

    def configure_options(self):
        super(AggJob, self).configure_options()

        self.add_passthrough_option('--output-path', help="Specify output path",
                                    default=None)
        self.add_passthrough_option('--start-date', help="Specify start date",
                                    default=None)

    def mapper(self, key, value):
        return map(self, key, value)

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
    outfd = open(path, "w")
    csvw = csv.writer(outfd)
    for k, v in getresults(fd):
        csvw.writerow(v)

if __name__ == '__main__':
    AggJob.run()
