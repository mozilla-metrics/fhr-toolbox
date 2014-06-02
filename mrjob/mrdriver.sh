# install mrjob

export https_proxy=http://proxy.dmz.scl3.mozilla.com:3128
export http_proxy=http://proxy.dmz.scl3.mozilla.com:3128

pip install --user mrjob

# fetch & run
# refs : http://mrjob.readthedocs.org/en/latest/guides/writing-mrjobs.html

mkdir $HOME/tmp 2>/dev/null
export TMPDIR=$HOME/tmp

HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce python fhr-toolbox/jydoop/aggregate-collection.py --runner hadoop --jobconf mapred.reduce.tasks=20 --file ~/fhr-toolbox/jydoop/healthreportutils.py --start-date=2014-05-12 --hadoop-bin /usr/bin/hadoop hdfs:///user/bcolloran/fhrDeorphaned_2014-05-12

# to test etc:
# hadoop dfs -text /user/sguha/fhr/samples/output/5pct/part-r-00072 | head -n 10000 | python -m cProfile aggregate-collection.py - > outfile
