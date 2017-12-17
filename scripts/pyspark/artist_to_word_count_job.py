import collections
import constants
import happybase
import json
import os
import re
import shared
import pyspark.conf
import pyspark.context

APP_NAME = 'artist_to_word_count_job'

def emit_lyricid_word_pairs(line, ngram):
    """ This function takes a JSON string and return a key-value pair.

    The returned key is the lyric id while the returned value is a list of ngram-word.
    The "n" here can be 1 or 2.
    """
    line_json = json.loads(line.strip())
    lyric_id, lyric_text = line_json['lyricid'], line_json['lyrics']
    words = re.split('\n| ', lyric_text)

    normalized_words = []
    for word in words:
        normalized_word = shared.normalize_word(word)
        if normalized_word:
            normalized_words.append(normalized_word)

    return (lyric_id, [' '.join(normalized_words[i:i+ngram]) for i in xrange(len(normalized_words)+1-ngram)])

def map_lyricid_to_artistname(partition):
    """ This function works on a RDD partition with lyric id-lyric text pairs and
    transform it to artist name-lyric text pairs.

    One lyric id can be mapped to multiple artist names. This function queries the
    Hbase database to fetech all artist names associated with the given lyric id.
    The expensive Hbase connection cost is offset by reusing one Hbase connection
    in each partition.
    """
    connection = happybase.Connection(constants.HBASE_THRIFT_HOST, constants.HBASE_PORT)
    table = connection.table(constants.LYRICS_TO_ARTISTS_TABLE)

    ret_partition = []
    for lyric_id, lyric_text in partition:
        row = table.row(lyric_id)
        ret_partition.extend([(artist_name, lyric_text) for artist_name in row if artist_name])

    return iter(ret_partition)

def compute_word_count(args):
    """ This function maps each (artist, list of word) pair to (artist, list of (word, count)) pair.
    """
    artist_name, lyric_text = args
    word_counter = collections.Counter(lyric_text)
    return (artist_name, word_counter.items())

def etl_workflow(sc, ngram, hdfs_path, hbase_table, shared_trivial_words):
    """ This function ETL the raw lyrics data on HDFS and saves the results to Hbase.
    """
    try:
        lyrics_to_words_rdd = sc.textFile(hdfs_path) \
                                .map(lambda line: eval(line))
        lyrics_to_words_rdd.take(1)
    except Exception, err:
        lyrics_to_words_rdd = sc.textFile(constants.RAW_LYRICS_HDFS_PATH) \
                                .map(lambda line: emit_lyricid_word_pairs(line, ngram)) \
                                .mapPartitions(map_lyricid_to_artistname) \
                                .reduceByKey(lambda a, b: a + b) \
                                .map(compute_word_count)
        lyrics_to_words_rdd.saveAsTextFile(hdfs_path)
    lyrics_to_words_rdd.foreachPartition(lambda partition: shared.bulk_insert_words_to_artists_count(partition, shared_trivial_words.value, 'words', hbase_table))

def main():
    conf = pyspark.conf.SparkConf()
    conf.setAppName(APP_NAME)
    sc = pyspark.context.SparkContext(conf=conf)
    sc.addPyFile("dep.zip")

    trivial_single_words = shared.fetch_all_row_keys(constants.SINGLE_WORD_MOST_FREQUENT_WORDS_TABLE)
    shared_trivial_single_words = sc.broadcast(trivial_single_words)
    trivial_two_gram_words = shared.fetch_all_row_keys(constants.TWO_GRAM_MOST_FREQUENT_WORDS_TABLE)
    shared_trivial_two_gram_words = sc.broadcast(trivial_two_gram_words)

    etl_workflow(sc, 1, constants.SINGLE_WORD_NORM_ARTISTS_WORDS_COUNT_HDFS_PATH,
                        constants.SINGLE_WORD_ARTISTS_WORDS_COUNT_TABLE,
                        shared_trivial_single_words)
    etl_workflow(sc, 2, constants.TWO_GRAM_NORM_ARTISTS_WORDS_COUNT_HDFS_PATH,
                        constants.TWO_GRAM_ARTISTS_WORDS_COUNT_TABLE,
                        shared_trivial_two_gram_words)

if __name__ == '__main__':
    main()
