import sys, csv

usactive = {}
nonusactive = {}

r = csv.reader(sys.stdin)
for geo, active, search, count in r:
    count = int(count)
    if active != "True":
        continue

    if geo == "US":
        d = usactive
    else:
        d = nonusactive

    if not search in d:
        d[search] = 0
    d[search] += count

print "Latest search provider per user"

l = usactive.items()
l.sort(key=lambda i: i[1], reverse=True)
total = sum(count for s, count in l)
print "beta channel, en-US locale, US geo: %i active users" % (total,)
for s, count in l:
    if count < 1000:
        break
    print "  %i: %s" % (count, s)

l = nonusactive.items()
l.sort(key=lambda i: i[1], reverse=True)
total = sum(count for s, count in l)
print "beta channel, en-US locale, non-US geo: %i active users" % (total,)
for s, count in l:
    if count < 1000:
        break
    print "  %i: %s" % (count, s)
