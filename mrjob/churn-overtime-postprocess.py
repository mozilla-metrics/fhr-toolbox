import csv
import sys
import os
from collections import defaultdict

basedir, = sys.argv[1:]

def aggregate(inpath, outpath):
    weeks = defaultdict(lambda: 0)
    r = csv.reader(open(inpath))
    for date, os, locale, geo, count in r:
        weeks[date] += int(count)

    weeks = weeks.items()
    weeks.sort(key=lambda i: i[0])
    w = csv.writer(open(outpath, "w"))
    for i in weeks:
        w.writerow(i)

aggregate(os.path.join(basedir, "loss.csv"), os.path.join(basedir, "loss-simple.csv"))
aggregate(os.path.join(basedir, "gain.csv"), os.path.join(basedir, "gain-simple.csv"))
