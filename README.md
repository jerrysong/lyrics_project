# lyrics_project
Track favorite words of your favorite artist!

Dependency:
Yum:
yum install python-devel
yum install nodejs

Python:
pip install happybase
pip install pyspark
pip install flask
pip install raven
pip install numpy

NodeJS:
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
