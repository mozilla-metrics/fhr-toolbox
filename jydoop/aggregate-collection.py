"""
Collect all sorts of aggregate data from a single mapreduce run.

Usage stats:
* user type: active, new (with date), returning (with date), lost (with date)
* usage days over the past four Sunday->Saturday weeks
* usage hours (bucketed) over the past four Sunday->Saturday weeks

State data:
* locale/geo breakdown
* telemetry-enabled breakdown
* updates-enabled breakdown
* addonid/addonname breakdown

All data separated by channel and filtered for the main channels. Partner
channels grouped into the main channels.
"""

import healthreportutils
from datetime import date, datetime, timedelta
import os, shutil, csv
import sys, codecs

import mrjob
import tempfile

try:
    import simplejson as json
except ImportError:
    import json

# How many days must a user be gone to be considered "lost"?
LOSS_DAYS = 7 * 6 # 42 days/one release cycle
TOTAL_DAYS = 180

main_channels = (
    'nightly',
    'aurora',
    'beta',
    'release'
)

date_key = "org.mozilla.fhrtoolbox.snapshotdate"
def last_saturday(d):
    """Return the Saturday on or before the date."""
    # .weekday in python starts on 0=Monday
    return d - timedelta(days=(d.weekday() + 2) % 7)

def start_date(dstr):
    """
    Start measuring a few days before the snapshot was taken to give clients
    time to upload.
    """
    return datetime.strptime(dstr, "%Y-%m-%d").date() - timedelta(days=2)

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
            yield("size", len(v))
            yield(("bucketsize", len(v) / 1000), 1)

            for k1, v1 in func(job, k, v):
                yield(k1, v1)
        except Exception as e:
            yield("exception", (str(e), v))
    return wrapper

@logexceptions
@healthreportutils.FHRMapper()
def map(job, key, payload):
    channel = payload.channel.split("-")[0]
    if channel not in main_channels:
        return

    days = payload.get('data', {}).get('days', {})
    def get_day(d):
        dstr = d.strftime("%Y-%m-%d")
        return days.get(dstr, None)

    first_active = None
    last_info = None

    sd = start_date(job.options.start_date)
    for d in date_back(sd, LOSS_DAYS):
        day = get_day(d)
        if active_day(day):
            first_active = d
            if not last_info and "org.mozilla.appInfo.appinfo" in day:
                last_info = day

    if first_active:
        # Discern active/new/returning
        for d in date_back(first_active - timedelta(days=1), LOSS_DAYS):
            if active_day(get_day(d)):
                yield(("users", channel, "active", ""), 1)
                break
        else:
            for d in date_back(first_active - timedelta(LOSS_DAYS + 1), TOTAL_DAYS):
                if active_day(get_day(d)):
                    yield(("users", channel, "return", first_active.strftime("%Y-%m-%d")), 1)
                    break
            else:
                yield(("users", channel, "new", first_active.strftime("%Y-%m-%d")), 1)
    else:
        for d in date_back(sd - timedelta(days=LOSS_DAYS), LOSS_DAYS):
            if active_day(get_day(d)):
                yield(("users", channel, "lost", d.strftime("%Y-%m-%d")), 1)
                break
        return # no other stats if user wasn't active

    def write_week(ending):
        days = 0
        ticks = 0.0
        for d in date_back(ending, 7):
            day = get_day(d)
            if active_day(day):
                days += 1
                sessions = day.get("org.mozilla.appSessions.previous", None)
                if sessions is None:
                    continue
                dticks = sum(sessions.get("cleanActiveTicks", [])) + \
                    sum(sessions.get("abortedActiveTicks", []))
                ticks += dticks

        # bucket ticks by hour
        hours = int(round(ticks * 5 / 60 / 60, 1))
        yield(("days", channel, ending.strftime("%Y-%m-%d"), days), 1)
        yield(("ticks", channel, ending.strftime("%Y-%m-%d"), hours), 1)

    week_end = last_saturday(sd)
    for n in xrange(0, 4):
        write_week(week_end - timedelta(days=7 * n))

    # Addon and plugin data: require the v2 probes with correct names
    addons = payload.last.get("org.mozilla.addons.addons", {})
    addons_v = addons.get("_v", "?")
    if addons_v == 2:
        for addonid, data in addons.items():
            if addonid == "_v":
                continue
            yield(("addons", channel, addonid,
                           data.get("userDisabled", "?"),
                           data.get("appDisabled", "?"),
                           data.get("name", "?")), 1)

    plugins = payload.last.get("org.mozilla.addons.plugins", {})
    plugins_v = plugins.get("_v", "?")
    if plugins_v == 1:
        for pluginid, data in plugins.items():
            if pluginid == "_v":
                continue
            yield(("plugins", channel,
                           data.get("name", "?"),
                           data.get("blocklisted", "?"),
                           data.get("disabled", "?"),
                           data.get("clicktoplay", "?")), 1)

    # everything else
    if not last_info:
        last_info = {}

    version = payload.get("geckoAppInfo", {}).get("version", "?")
    locale = payload.last.get("org.mozilla.appInfo.appinfo", {}).get("locale", "?")
    default_browser = last_info.get("org.mozilla.appInfo.appinfo", {}).get("isDefaultBrowser", "?")
    telemetry = last_info.get("org.mozilla.appInfo.appinfo", {}).get("isTelemetryEnabled", "?")
    update_auto = last_info.get("org.mozilla.appInfo.update", {}).get("autoDownload", "?")
    update_enabled = last_info.get("org.mozilla.appInfo.update", {}).get("enabled", "?")
    geo = payload.get("geoCountry", "?")

    yield(("stats", channel, version, locale, default_browser, telemetry,
                   update_auto, update_enabled, geo, addons_v), 1)

def reduce(job, k, vlist):
    if k == "exception":
        print >> sys.stderr, "FOUND exception", vlist
        for v in vlist:
            yield(k, v)
    else:
        yield(k, sum(vlist))

class AggJob(mrjob.MRJob):
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
            outpath = os.path.expanduser("~/fhr-aggregates-" + self.options.start_date)
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
    errs = codecs.getwriter("utf-8")(open(os.path.join(path, "errors.txt"), "w"))
    for k, v in getresults(fd):
        if k == "exception":
            print >>errs, "==ERR=="
            print >>errs, v[0]
            print >>errs, v[1]
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
