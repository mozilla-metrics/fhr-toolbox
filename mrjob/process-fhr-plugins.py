import sys, os, csv
from collections import defaultdict, Counter


fhrdir, = sys.argv[1:]

targetchannel = 'release'
sample = 0.01

oslist = (
    'WINNT',
    'Darwin',
    'Linux',
)

tfmap = {
    "True": True,
    "False": False,
    "?": None,
}
def tf(v):
    """Convert a string to True/False or None if unparseable"""
    r = tfmap.get(v, None)
    if r is None:
        print >>sys.stderr, "Unexpected t/f value %r" % (v,)
    return r

statsdir = os.path.join(fhrdir, 'totals.csv')
pluginsdir = os.path.join(fhrdir, 'plugins.csv')

totals = Counter() # os

r = csv.reader(open(statsdir))
for channel, os, count in r:
    if targetchannel != channel:
        continue
    if not os in oslist:
        continue
    totals[os] += int(count)

#print "Active users by OS, %s channel:" % (targetchannel,)
#for os in oslist:
#    print "  %s: %i" % (os, totals[os] / sample)
#print

counts = defaultdict(Counter) # (os, state)
versions = defaultdict(Counter) # (os, version)

r = csv.reader(open(pluginsdir))
for channel, os, name, version, blocklisted, disabled, clicktoplay, count in r:
    if targetchannel != channel:
        continue
    if not os in oslist:
        continue
    if name != "Shockwave Flash":
        continue
    blocklisted = tf(blocklisted)
    disabled = tf(disabled)
    clicktoplay = tf(clicktoplay)
    count = int(count)
    if disabled:
        state = "disabled"
    elif blocklisted:
        state = "blocklisted"
    elif clicktoplay:
        state = "ctp"
    elif blocklisted is None or disabled is None or clicktoplay is None:
        state = "unknown"
    else:
        state = "active"
    counts[os][state] += count
    versions[os][version] += count

for os in oslist:
    print os
    print "  By Status"

    oscounts = counts[os]

    notpresent = totals[os] - sum(oscounts.values())
    oscounts['notpresent'] = notpresent

    oscounts = oscounts.items()
    oscounts.sort(reverse=True, key=lambda i: i[1])

    for state, count in oscounts:
        print "    %s: %.1f%%" % (state, float(count) / totals[os] * 100)

    print "  By Version"
    osversions = versions[os].items()
    osversions.sort(reverse=True, key=lambda i: i[1])
    versiontotal = sum(count for version, count in osversions)

    for version, count in osversions:
        ratio = float(count) / versiontotal * 100
        if ratio < 0.5:
            break
        print "    %s: %.1f%%" % (version, ratio)

    print
