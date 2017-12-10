# lyrics_project
Track favorite words of your favorite artist!

Dependency:
Yum:
yum install python-devel

Python:
pip install happybase
pip install pyspark

Start Services:
Hadoop:
/usr/local/hadoop/sbin/start-all.sh

HBase:
/usr/local/hbase/bin/hbase-daemon.sh start thrift -p 9090 --infoport 9095

Spark:
/usr/local/spark/sbin/start-all.sh

Run Jobs:
sh /usr/local/spark/bin/spark-submit artists_mapreduce.py -master spark://50.23.83.242:7077 --executor-memory 10G --total-executor-cores 100 1000
sh /usr/local/spark/bin/spark-submit lyric_mapreduce.py -master spark://50.23.83.242:7077 --executor-memory 10G --total-executor-cores 100 1000
