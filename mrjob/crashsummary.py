import sys, os, csv
from collections import defaultdict, Counter
from datetime import date, datetime, timedelta

crashdir, = sys.argv[1:]

targetchannel = 'release'
targetos = 'WINNT'

def opencsv(name):
    return csv.reader(open(os.path.join(crashdir, name + '.csv')))

def dstr_to_date(dstr):
    return datetime.strptime(dstr, "%Y-%m-%d").date()

class Day(object):
    def __init__(self):
        self.active = 0
        self.seconds = 0
        self.ticks = 0
        self.crashes = defaultdict(int)
        self.submissions = defaultdict(int)

days = defaultdict(Day)

totalactives = 0
totalseconds = 0
totalticks = 0

for dstr, channel, os_, count in opencsv("daily-active"):
    if channel != targetchannel:
        continue
    if os_ != targetos:
        continue
    d = dstr_to_date(dstr)
    count = int(count)
    days[d].active += count
    totalactives += count

for dstr, channel, os_, count in opencsv("daily-seconds"):
    if channel != targetchannel:
        continue
    if os_ != targetos:
        continue
    d = dstr_to_date(dstr)
    count = int(count)
    days[d].seconds += count
    totalseconds += count

for dstr, channel, os_, count in opencsv("daily-ticks"):
    if channel != targetchannel:
        continue
    if os_ != targetos:
        continue
    d = dstr_to_date(dstr)
    count = int(count)
    days[d].ticks += count
    totalticks += count

crashesbytype = defaultdict(int)
submissionsbytype = defaultdict(int)
submitfailbytype = defaultdict(int)

for dstr, type, channel, os_, count in opencsv("daily"):
    if channel != targetchannel:
        continue
    if os_ != targetos:
        continue
    d = dstr_to_date(dstr)
    count = int(count)
    days[d].crashes[type] += count
    crashesbytype[type] += count

for dstr, type, channel, os_, count in opencsv("daily-submission-succeeded"):
    if channel != targetchannel:
        continue
    if os_ != targetos:
        continue
    d = dstr_to_date(dstr)
    count = int(count)
    days[d].submissions[type] += count
    submissionsbytype[type] += count

for dstr, type, channel, os_, count in opencsv("daily-submission-failed"):
    if channel != targetchannel:
        continue
    if os_ != targetos:
        continue
    d = dstr_to_date(dstr)
    count = int(count)
    submitfailbytype[type] += count

class SparseList(list):
  def __setitem__(self, index, value):
    missing = index - len(self) + 1
    if missing > 0:
      self.extend([0] * missing)
    list.__setitem__(self, index, value)
  def __getitem__(self, index):
    try: return list.__getitem__(self, index)
    except IndexError: return 0

crashes = SparseList()
pcrashes = SparseList()

for channel, os_, main, plugin, phang, gmplugin, content, count in opencsv("totals"):
    if channel != targetchannel:
        continue
    if os_ != targetos:
        continue
    main = int(main)
    plugin = int(plugin) + int(phang)
    count = int(count)
    crashes[main] += count
    pcrashes[plugin] += count

wincrashes = crashes
winpcrashes = pcrashes

crashtotal = float(sum(wincrashes))
pcrashtotal = float(sum(winpcrashes))

cutoff = 8

maincount = crashesbytype["main-crash"]
plugincount = crashesbytype["plugin-crash"] + crashesbytype["plugin-hang"]
mainsubmissions = submissionsbytype["main-crash"]
pluginsubmissions = submissionsbytype["plugin-crash"] + submissionsbytype["plugin-hang"]

print "All data for OS '%s' and release channel '%s'" % (targetos, targetchannel)
print
print "Main-process crashes per active day (MCPD-main): %.4f" % (
    maincount / float(totalactives),)

print "Plugin crashes per active day (MCPD-p): %.4f" % (
    plugincount / float(totalactives),)

print "%% of plugin crashes which are hangs: %.1f" % (
    crashesbytype["plugin-hang"] / float(plugincount) * 100,)

print "Mean session hours between main-process crashes (MTBF-main): %.1f" % (
    totalseconds / 60.0 / 60 / maincount,)

print "Mean session hours between plugin-process crashes (MTBF-p): %.1f" % (
    totalseconds / 60.0 / 60 / plugincount,)

print "Mean active hours between main-process crashes (MABF-main): %.1f" % (
    totalticks / 12.0 / 60 / maincount,)

print "Mean active hours between plugin-process crashes (MABF-p): %.1f" % (
    totalticks / 12.0 / 60 / plugincount,)

print "Submission rate for main-process crashes: %.1f%%" % (
    float(mainsubmissions) / maincount * 100,)
print "Submission rate for plugin crashes: %.1f%%" % (
    float(pluginsubmissions) / plugincount * 100,)

print "Failed submissions for main-process crashes: %.1f%%" % (
    float(submitfailbytype["main-crash"]) / (submitfailbytype["main-crash"] + mainsubmissions) * 100,)

print
print "Per day:"
print "%10s %10s %10s %10s %10s %10s %10s %10s %10s" % ("Day", "MCPD-main", "MCPD-p", "MTBF-main", "MTBF-p", "MABF-main", "MABF-p", "MSubmit", "PSubmit")

daylist = days.keys()
daylist.sort()
for day in daylist:
    d = days[day]
    maincount = d.crashes["main-crash"]
    plugincount = d.crashes["plugin-crash"] + d.crashes["plugin-hang"]

    mainsubmissions = d.submissions["main-crash"]
    pluginsubmissions = d.submissions["plugin-crash"] + d.submissions["plugin-hang"]

    print "%10s %10.4f %10.4f %10.1f %10.1f %10.1f %10.1f %9.1f%% %9.1f%%" % (
        day.strftime("%A"),
        maincount / float(d.active),
        plugincount / float(d.active),
        d.seconds / 60.0 / 60 / maincount,
        d.seconds / 60.0 / 60 / plugincount,
        d.ticks / 12.0 / 60 / maincount,
        d.ticks / 12.0 / 60 / plugincount,
        float(mainsubmissions) / maincount * 100,
        float(pluginsubmissions) / plugincount * 100,
    )

print
print "For all active users week:"
print "main-process crashes per user:"
for c in range(0, cutoff):
    print "%i: %.2f%%" % (c, wincrashes[c] / crashtotal * 100)
print "More than %i: %.2f%%" % (cutoff, sum(wincrashes[cutoff:]) / crashtotal * 100)

print
print "plugin crashes or hangs, per user:"
for c in range(0, cutoff):
    print "%i: %.2f%%" % (c, winpcrashes[c] / pcrashtotal * 100)
print "More than %i: %.2f%%" % (cutoff, sum(winpcrashes[cutoff:]) / pcrashtotal * 100)
