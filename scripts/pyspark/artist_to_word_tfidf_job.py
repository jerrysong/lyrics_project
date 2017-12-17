import collections
import constants
import happybase
import json
import os
import re
import shared
import pyspark.conf
import pyspark.context

APP_NAME = 'artist_to_word_tfidf_job'

def flat_map_to_word_count(args):
    """ This function maps each (artist, list of (word, count)) pair to multiple (word, (artist, count)) pairs.
    """
    artist_name, word_counts = args
    ret = []

    for word, count in word_counts:
        ret.append((word, (artist_name, count)))
    return ret

def word_count_map_to_tfidf(args):
    """ This function maps a word's count to its term frequency-inverse document frequency.

    Two special rules apply here: If the corpus count of a word is less than 10, or the local
    count of a word is equivalent to its corpus count, then the tf-idf will be zero instead.
    """
    word, artist, count, total_count = args[0], args[1][0][0], args[1][0][1], args[1][1]
    tfidf_factor, threshold = 1000000, 10
    tfidf = 0.0 if (total_count < threshold or count == total_count) else (float(count) / float(total_count) * tfidf_factor)
    return (artist, [(word, tfidf)])

def etl_workflow(sc, ngram, words_count_hdfs_path, lyrics_to_words_hdfs_path, tfidf_hdfs_path, hbase_table, shared_trivial_words):
    """ This function ETL the raw lyrics data on HDFS and saves the results to Hbase.
    """
    try:
        tfidf_rdd = sc.textFile(tfidf_hdfs_path) \
                      .map(lambda line: eval(line))
        tfidf_rdd.take(1)
    except Exception, err:
        lyrics_to_words_rdd = sc.textFile(lyrics_to_words_hdfs_path) \
                                .map(lambda line: eval(line))
        words_count_rdd = sc.textFile(words_count_hdfs_path) \
                            .map(lambda line: eval(line))
        tfidf_rdd = lyrics_to_words_rdd.flatMap(flat_map_to_word_count) \
                                       .join(words_count_rdd) \
                                       .map(word_count_map_to_tfidf) \
                                       .reduceByKey(lambda a, b: a + b)
        tfidf_rdd.saveAsTextFile(tfidf_hdfs_path)
    tfidf_rdd.foreachPartition(lambda partition: shared.bulk_insert_words_to_artists_count(partition, shared_trivial_words.value, 'words_tf_idf', hbase_table))

def main():
    conf = pyspark.conf.SparkConf()
    conf.setAppName(APP_NAME)
    sc = pyspark.context.SparkContext(conf=conf)
    sc.addPyFile("dep.zip")

    trivial_single_words = shared.fetch_all_row_keys(constants.SINGLE_WORD_MOST_FREQUENT_WORDS_TABLE)
    shared_trivial_single_words = sc.broadcast(trivial_single_words)
    trivial_two_gram_words = shared.fetch_all_row_keys(constants.TWO_GRAM_MOST_FREQUENT_WORDS_TABLE)
    shared_trivial_two_gram_words = sc.broadcast(trivial_two_gram_words)

    etl_workflow(sc, 1, constants.SINGLE_WORD_CORPUS_WORDS_COUNT_HDFS_PATH,
                        constants.SINGLE_WORD_NORM_ARTISTS_WORDS_COUNT_HDFS_PATH,
                        constants.SINGLE_WORD_NORM_ARTISTS_WORDS_TFIDF_HDFS_PATH,
                        constants.SINGLE_WORD_ARTISTS_WORDS_COUNT_TABLE,
                        shared_trivial_single_words)
    etl_workflow(sc, 2, constants.TWO_GRAM_CORPUS_WORDS_COUNT_HDFS_PATH,
                        constants.TWO_GRAM_NORM_ARTISTS_WORDS_COUNT_HDFS_PATH,
                        constants.TWO_GRAM_NORM_ARTISTS_WORDS_TFIDF_HDFS_PATH,
                        constants.TWO_GRAM_ARTISTS_WORDS_COUNT_TABLE,
                        shared_trivial_two_gram_words)

if __name__ == '__main__':
    main()
