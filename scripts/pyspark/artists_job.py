import constants
import json
import os
import pyspark.conf
import pyspark.context
import happybase

APP_NAME = 'artists_job'

def load_and_extract(line):
    line_json = json.loads(line.strip())
    lyric_id = line_json['lyricid']
    if 'artists' in line_json:
        return [(lyric_id, artist_json['artistname'].lower().encode("ascii", "ignore")) for artist_json in line_json['artists']]
    elif 'artist' in line_json:
        return [(lyric_id, line_json['artist']['artistname'].lower().encode("ascii", "ignore"))]
    else:
        return []

def bulk_insert(partition):
    connection = happybase.Connection(constants.MASTER_HOST, constants.HBASE_PORT)
    batch = connection.table(constants.LYRICS_TO_ARTISTS_TABLE).batch(batch_size = 1000)

    for lyric_id, artist_name in partition:
        data = { 'artists:%s' % (artist_name,) : None }
        batch.put(lyric_id, data)

def main():
    conf = pyspark.conf.SparkConf()
    conf.setAppName(APP_NAME)
    sc = pyspark.context.SparkContext(conf=conf)
    lyrics_to_artists_rdd = sc.textFile('hdfs://%s:%s/resources/raw_data/raw_artists.txt' % (constants.MASTER_HOST, constants.HDFS_PORT)) \
                              .flatMap(load_and_extract) \
                              .foreachPartition(bulk_insert)

if __name__ == "__main__":
    main()
