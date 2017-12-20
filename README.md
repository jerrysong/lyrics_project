# Lyrics Project
Track favorite words of your favorite artist!

## This project contains following components:
1. A Hadoop Cluster: HDFS is used to store raw lyrics and artists data in JSON format.
2. Hbase: used as a persistent database.
3. Hbase Thrift Server: used to serialize and diserialize the data flow into / out from Hbase.
4. Spark: to process raw data and save the results into Hbase.
5. Yarn: as the resource manger and job coordinator of the Hadoop Cluster.
6. Flask Backend Server: to communicate with the Hbase database and service queries from the front end.
7. Bootstrap, JQuery and D3.js: for powering the front end.

## Dependency
Following dependencies are prerequisites and need to be installed first:

    yum install python-devel

    pip install happybase
    pip install pyspark
    pip install flask
    pip install raven
    pip install numpy
    pip install gunicorn


## Building the Cluster
Please refer to the official documentation of Hadoop, Spark and Hbase regarding how to build a cluster. This project uses the latest version of them. Please install them in `/usr/local` directory.
To start the cluster, the following scripts need to be run:

    /usr/local/hadoop/sbin/start-all.sh
    /usr/local/hbase/bin/start-hbase.sh
    /usr/local/hbase/bin/hbase-daemon.sh start thrift -p 9090 --infoport 9095
    /usr/local/spark/sbin/start-all.sh

## Running Spark Jobs
Before running spark jobs, the raw data has to be downloaded from s3://w251lyrics-project/lyric.json.gz and s3://w251lyrics-project/full_US.json.gz and saved to HDFS `/resources/raw_data` directory as `raw_lyrics.txt` and `raw_artists.txt` respectively. The environment variables should be exported as `source ~/lyrics_project/scripts/env_setup.sh`.

    sh /usr/local/spark/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 12 --executor-memory 6G --py-files /root/lyrics_project/scripts/pyspark/dep.zip /root/lyrics_project/scripts/pyspark/lyricid_to_artist_job.py

    sh /usr/local/spark/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 12 --executor-memory 6G --py-files /root/lyrics_project/scripts/pyspark/dep.zip /root/lyrics_project/scripts/pyspark/corpus_word_count_job.py

    sh /usr/local/spark/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 12 --executor-memory 6G --py-files /root/lyrics_project/scripts/pyspark/dep.zip /root/lyrics_project/scripts/pyspark/artist_to_word_count_job.py

    sh /usr/local/spark/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 12 --executor-memory 6G --py-files /root/lyrics_project/scripts/pyspark/dep.zip /root/lyrics_project/scripts/pyspark/artist_to_word_tfidf_job.py

## Starting the Web Server:
This github repository needs to be checked out locally on the isntance or server and `lyrics_project/app` should be the present working directory. The web server in can be started in the background:

    gunicorn -w 4 -b <host_ip_address>:80 server:app -D
