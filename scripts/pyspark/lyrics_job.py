import collections
import constants
import happybase
import json
import os
import re
import heapq
import pyspark.conf
import pyspark.context
import pyspark.ml.feature

APP_NAME = 'lyrics_job'

def normalize_word(word):
    """ This function normalizes a single word.

    It transforms all letters to lowercase, and strip leading and trailing
    non-alphanumeric characters.
    """
    word = word.lower()
    left = 0
    while left < len(word) and not word[left].isalpha():
        left += 1
    right = len(word)-1
    while right > left and not word[right].isalpha():
        right -= 1

    return word[left:right+1]

def flat_map_words(line):
    """ This functions takes a JSON string and returns a flat list of key-value pairs.

    This function extracts the lyric text from the JSON object. Split the lyric text
    into a list of words and normalize them. Count the occurence of each word.
    Lastly, return a list of (word, count) pairs.
    """
    line_json = json.loads(line.strip())
    lyric_id, lyric_text = line_json['lyricid'], line_json['lyrics']
    words = re.split('\n| ', lyric_text)

    normalized_words = collections.Counter()
    for word in words:
        normalized_word = normalize_word(word)
        if normalized_word:
            normalized_words[normalized_word] += 1
    return normalized_words.items()

def is_valid_record(line):
    """ Validate the input line.

    The input is valid only if it can be serialized into a JSON object and contains
    the expected keys.
    """
    line_json = None
    try:
        line_json = json.loads(line.strip())
    except ValueError, err:
        return False
    return 'lyricid' in line_json and 'lyrics' in line_json

def load_and_extract(line, ngram=1):
    """ This function takes a JSON string and return a key-value pair.

    The returned key is the lyric id while the returned value is a list of ngram-word.
    The "n" here can be 1, 2 or 3.
    """
    line_json = json.loads(line.strip())
    lyric_id, lyric_text = line_json['lyricid'], line_json['lyrics']
    words = re.split('\n| ', lyric_text)

    lyric_text = []
    for word in words:
        normalized_word = normalize_word(word)
        if normalized_word:
            lyric_text.append(normalized_word)

    if ngram == 1:
        return (lyric_id, lyric_text)
    else:
        return (lyric_id, [tuple(lyric_text[i:i+ngram]) for i in xrange(len(lyric_text)+1-ngram)])

def compute_word_count(args):
    """ This function maps each (artist, list of word) pair to (artist, list of (word, count)) pair.
    """
    artist_name, lyric_text = args
    word_counter = collections.Counter(lyric_text)
    return (artist_name, word_counter.items())

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

def bulk_insert_words_to_artists_count(partition, trivial_words, column_prefix, table_name):
    """ This function batch insert all (artist, list of (word, count / tfidf)) pairs in
    one partition to the Hbase database.

    A word in this context can be a single word, a 2-gram word tuple or a 3-gram word tuple. Each
    Hbase table contains six column families: column family with all words' count, column family
    with all words' tfidf, column family with top 10 frequent words' count, column family with
    top 10 frequent words' tfidf, column family with top 10 nontrival frequent words' count,
    column family with top 10 frequent nontrival words' tfidf. A word is considered trival if it
    is found in a predefined trival words set.
    """
    batch = happybase.Connection(constants.HBASE_THRIFT_HOST, constants.HBASE_PORT).table(table_name).batch(batch_size = 1000)

    for artist_name, word_counts in partition:
        count_data = { '%s:%s' % (column_prefix, word):count for word, count in word_counts }

        top_words = heapq.nlargest(110, count_data.iteritems(), key=lambda t:float(t[1]))
        for word, count in top_words[:10]:
            count_data['top_10_%s' % (word,)] = count

        cnt = 0
        for word, count in top_words:
            if word.split(':')[1] not in trivial_words:
                count_data['top_10_nontrival_%s' % (word,)] = count
                cnt += 1
            if cnt == 10:
                break

        count_data = { key:str(value) for key, value in count_data.iteritems() }
        batch.put(artist_name, count_data)

def bulk_insert_words_to_words_count(partition):
    """ This function inserts the corpus count of each word to the Hbase database.
    """
    connection = happybase.Connection(constants.HBASE_THRIFT_HOST, constants.HBASE_PORT)
    batch = connection.table(constants.SINGLE_WORD_CORPUS_WORDS_COUNT_TABLE).batch(batch_size = 1000)
    for word, count in partition:
        data = { 'counts' : str(count) }
        batch.put(word, data)

def main():
    conf = pyspark.conf.SparkConf()
    conf.setAppName(APP_NAME)
    sc = pyspark.context.SparkContext(conf=conf)

    remover = pyspark.ml.feature.StopWordsRemover()
    trivial_words = set(remover.loadDefaultStopWords('english'))
    shared_trivial_words = sc.broadcast(trivial_words)

    try:
        words_count_rdd = sc.textFile(constants.SINGLE_WORD_NORM_WORDS_COUNT_HDFS_PATH) \
                            .map(lambda line: eval(line))
        words_count_rdd.take(1)
    except Exception, err:
        words_count_rdd = sc.textFile(constants.RAW_LYRICS_HDFS_PATH) \
                            .flatMap(flat_map_words) \
                            .reduceByKey(lambda a, b: a + b)
        words_count_rdd.saveAsTextFile(constants.SINGLE_WORD_NORM_WORDS_COUNT_HDFS_PATH)
    words_count_rdd.foreachPartition(bulk_insert_words_to_words_count)

    try:
        lyrics_to_words_rdd = sc.textFile(constants.SINGLE_WORD_NORM_ARTISTS_WORDS_COUNT_HDFS_PATH) \
                                .map(lambda line: eval(line))
        lyrics_to_words_rdd.take(1)
    except Exception, err:
        lyrics_to_words_rdd = sc.textFile(constants.RAW_LYRICS_HDFS_PATH) \
                                .filter(is_valid_record) \
                                .map(load_and_extract) \
                                .mapPartitions(map_lyricid_to_artistname) \
                                .reduceByKey(lambda a, b: a + b) \
                                .map(compute_word_count)
        lyrics_to_words_rdd.saveAsTextFile(constants.SINGLE_WORD_NORM_ARTISTS_WORDS_COUNT_HDFS_PATH)
    lyrics_to_words_rdd.foreachPartition(lambda partition: bulk_insert_words_to_artists_count(partition, shared_trivial_words.value, 'words', constants.SINGLE_WORD_ARTISTS_WORDS_COUNT_TABLE))

    try:
        tfidf_rdd = sc.textFile(constants.SINGLE_WORD_NORM_ARTISTS_WORDS_TFIDF_HDFS_PATH) \
                      .map(lambda line: eval(line))
        tfidf_rdd.take(1)
    except Exception, err:
        tfidf_rdd = lyrics_to_words_rdd.flatMap(flat_map_to_word_count) \
                                       .join(words_count_rdd) \
                                       .map(word_count_map_to_tfidf) \
                                       .reduceByKey(lambda a, b: a + b)
        tfidf_rdd.saveAsTextFile(constants.SINGLE_WORD_NORM_ARTISTS_WORDS_TFIDF_HDFS_PATH)
    tfidf_rdd.foreachPartition(lambda partition: bulk_insert_words_to_artists_count(partition, shared_trivial_words.value, 'words_tf_idf', constants.SINGLE_WORD_ARTISTS_WORDS_COUNT_TABLE))

if __name__ == '__main__':
    main()
