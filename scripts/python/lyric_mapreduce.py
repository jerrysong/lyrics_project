import collections
import happybase
import json
import os
import re
import heapq
import pyspark.conf
import pyspark.context

PROD_PATH = os.environ.get('PROD')
CLUSTER_CONFIG_PATH = PROD_PATH + '/conf/cluster_conf.json'
CLUSTER_CONFIG = json.loads(open(CLUSTER_CONFIG_PATH).read())
MASTER_HOST = CLUSTER_CONFIG['masterHost']
HBASE_PORT = CLUSTER_CONFIG['hbaseThriftPort']
HDFS_PORT = CLUSTER_CONFIG['hdfsMetadataPort']

APP_NAME = 'lyrics_mapreduce'
LYRICS_TO_ARTISTS_TABLE = 'lyrics_to_artists'
WORDS_COUNT_TABLE = 'artists_to_words_count'

def normalize_word(word):
    word = word.lower()
    left = 0
    while left < len(word) and not word[left].isalpha():
        left += 1
    right = len(word)-1
    while right > left and not word[right].isalpha():
        right -= 1

    return word[left:right+1]

def load_and_extract(line):
    line_json = json.loads(line.strip())
    lyric_id, lyric_text = line_json['lyricid'], line_json['lyrics']
    words = re.split('\n| ', lyric_text)

    normalized_words = []
    for word in words:
        normalized_word = normalize_word(word)
        if normalized_word:
            normalized_words.append(normalized_word)

    normalized_words_str = ' '.join(normalized_words)
    return (lyric_id, normalized_words_str)

def map_lyricid_to_artistname(partition):
    connection = happybase.Connection(MASTER_HOST, HBASE_PORT)
    table = connection.table(LYRICS_TO_ARTISTS_TABLE)

    ret_partition = []
    for lyric_id, lyric_text in partition:
        row = table.row(lyric_id)
        ret_partition.extend([(artist_name, lyric_text) for artist_name in row])

    return iter(ret_partition)

def compute_word_count(args):
    artist_name, lyric_text = args
    word_counter = collections.Counter(lyric_text.split(' '))
    word_counts = ['%s %s' % (key, value) for key, value in word_counter.iteritems()]
    word_counts_str = ' '.join(word_counts)
    return artist_name, word_counts_str

def bulk_insert(partition):
    connection = happybase.Connection(MASTER_HOST, HBASE_PORT)
    batch = connection.table(WORDS_COUNT_TABLE).batch(batch_size = 1000)

    for artist_name, word_counts_str in partition:
        word_counts = word_counts_str.split(' ')
        data = {}
        for i in xrange(0, len(word_counts), 2):
            word, count = word_counts[i], word_counts[i+1]
            data['words:%s' % (word,)] = count
        top_10_words = heapq.nlargest(10, data.iteritems(), key=lambda t:t[1])
        for word, count in top_10_words:
            data['top_10_%s' % (word,)] = count
        batch.put(artist_name, data)

def main():
    conf = pyspark.conf.SparkConf()
    conf.setAppName(APP_NAME)
    sc = pyspark.context.SparkContext(conf=conf)
    lyrics_to_words_rdd = sc.textFile('hdfs://%s:%s/resources/raw_data/raw_lyrics.txt' % (MASTER_HOST, HDFS_PORT)) \
                            .map(load_and_extract) \
                            .mapPartitions(map_lyricid_to_artistname) \
                            .reduceByKey(lambda a, b: '%s %s' % (a, b)) \
                            .map(compute_word_count) \
                            .foreachPartition(bulk_insert)

if __name__ == "__main__":
    main()
