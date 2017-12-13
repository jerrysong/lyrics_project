# Lyrics Project
Track favorite words of your favorite artist!

## This project contains following components:
1. A Hadoop Cluster. HDFS is used to store raw lyrics and artists data in JSON format.
2. Hbase. We use Hbase as a persistent database.
3. Hbase Thrift Server. We use thrift to serialize and diserialize the data flow into / out from Hbase.
4. Spark. We use Spark to process raw data and save the results into Hbase.
5. Yarn. We use Yarn as the resource manger and Spark job coordinator of the Hadoop Cluster.
6. Flask Backend Server. We build a backend server which talks to the Hbase database and answers queries from the front end.
7. Front End Page. We mainly use Bootstrap and JQuery in the front end.

## Dependency
Please install following software before you proceed.

    yum install python-devel
    yum install nodejs

    pip install happybase
    pip install pyspark
    pip install flask
    pip install raven
    pip install numpy

    npm install -g forever

Start Services:
Hadoop:
/usr/local/hadoop/sbin/start-all.sh

HBase:
/usr/local/hbase/bin/start-hbase.sh
/usr/local/hbase/bin/hbase-daemon.sh start thrift -p 9090 --infoport 9095

Spark:
/usr/local/spark/sbin/start-all.sh

Run Spark Jobs:
sh /usr/local/spark/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 4 --executor-memory 3G --verbose --py-files /root/lyrics_project/common/constants.py /root/lyrics_project/scripts/pyspark/artists_job.py
sh /usr/local/spark/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 4 --executor-memory 3G --verbose --py-files /root/lyrics_project/common/constants.py /root/lyrics_project/scripts/pyspark/lyrics_job.py

Start Web Server:
forever start -c python /root/lyrics_project/app/server.py
