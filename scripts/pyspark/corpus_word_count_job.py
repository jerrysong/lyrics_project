import collections
import constants
import happybase
import json
import re
import shared
import pyspark.conf
import pyspark.context

APP_NAME = 'corpus_words_count_job'

def flattern_words(line, ngram):
    """ This functions takes a JSON string and returns a flat list of ngram words.

    This function extracts the lyric text from the JSON object. Split the lyric text
    into a list of words and normalize them. A ngram word is a string of n words
    joined by empty spaces.
    """
    line_json = json.loads(line.strip())
    lyric_id, lyric_text = line_json['lyricid'], line_json['lyrics']
    words = re.split('\n| ', lyric_text)

    normalized_words = []
    for word in words:
        normalized_word = shared.normalize_word(word)
        if normalized_word:
            normalized_words.append(normalized_word)

    return [' '.join(normalized_words[i:i+ngram]) for i in xrange(len(normalized_words)+1-ngram)]

def bulk_insert_words_to_words_count(partition, table):
    """ This function inserts the corpus count of each word to the Hbase database.
    """
    connection = happybase.Connection(constants.HBASE_THRIFT_HOST, constants.HBASE_PORT)
    batch = connection.table(table).batch(batch_size = 100)
    for word, count in partition:
        data = { 'counts:count' : str(count) }
        batch.put(word, data)

def etl_workflow(sc, ngram, hdfs_path, corpus_count_table,frequent_word_table):
    """ This function ETL the raw lyrics data on HDFS. The corpus number of each ngram
    word is counted and saved to the Hbase database.
    """
    try:
        words_count_rdd = sc.textFile(hdfs_path) \
                            .map(lambda line: eval(line))
        words_count_rdd.take(1)
    except Exception, err:
        words_count_rdd = sc.textFile(constants.RAW_LYRICS_HDFS_PATH) \
                            .flatMap(lambda line: flattern_words(line, ngram)) \
                            .map(lambda word: (word, 1)) \
                            .reduceByKey(lambda a, b: a + b)
        words_count_rdd.saveAsTextFile(hdfs_path)

    words_count_rdd.foreachPartition(lambda partition: bulk_insert_words_to_words_count(partition, corpus_count_table))

    # Store the top 200 most frequent words to the database
    most_frequent_words = words_count_rdd.takeOrdered(200, key=lambda t: -t[1])
    bulk_insert_words_to_words_count(most_frequent_words, frequent_word_table)

def main():
    conf = pyspark.conf.SparkConf()
    conf.setAppName(APP_NAME)
    sc = pyspark.context.SparkContext(conf=conf)
    sc.addPyFile("dep.zip")

    etl_workflow(sc, 1, constants.SINGLE_WORD_CORPUS_WORDS_COUNT_HDFS_PATH,
                        constants.SINGLE_WORD_CORPUS_WORDS_COUNT_TABLE,
                        constants.SINGLE_WORD_MOST_FREQUENT_WORDS_TABLE)
    etl_workflow(sc, 2, constants.TWO_GRAM_CORPUS_WORDS_COUNT_HDFS_PATH,
                        constants.TWO_GRAM_CORPUS_WORDS_COUNT_TABLE,
                        constants.TWO_GRAM_MOST_FREQUENT_WORDS_TABLE)

if __name__ == '__main__':
    main()
