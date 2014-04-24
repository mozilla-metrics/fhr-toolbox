# install mrjob

export https_proxy=http://proxy.dmz.scl3.mozilla.com:3128
export http_proxy=http://proxy.dmz.scl3.mozilla.com:3128

pip install --user mrjob

# fetch & run
# refs : http://mrjob.readthedocs.org/en/latest/guides/writing-mrjobs.html

mkdir $HOME/tmp 2>/dev/null
export TMPDIR=$HOME/tmp

HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce/ python aggregate-collection.py  --runner hadoop --hadoop-bin /usr/bin/hadoop --jobconf mapred.reduce.tasks=20  --file healthreportutils.py  --start-date=2014-03-21 hdfs:///user/sguha/fhr/samples/output/5pct/

# to test etc:
# hadoop dfs -text /user/sguha/fhr/samples/output/5pct/part-r-00072 | head -n 10000 | python -m cProfile aggregate-collection.py - > outfile
