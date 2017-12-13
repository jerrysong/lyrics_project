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
    word = word.lower()
    left = 0
    while left < len(word) and not word[left].isalpha():
        left += 1
    right = len(word)-1
    while right > left and not word[right].isalpha():
        right -= 1

    return word[left:right+1]

def flat_map_words(line):
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
    line_json = None
    try:
        line_json = json.loads(line.strip())
    except ValueError, err:
        return False
    return 'lyricid' in line_json and 'lyrics' in line_json

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
    connection = happybase.Connection(constants.MASTER_HOST, constants.HBASE_PORT)
    table = connection.table(constants.LYRICS_TO_ARTISTS_TABLE)

    ret_partition = []
    for lyric_id, lyric_text in partition:
        row = table.row(lyric_id)
        ret_partition.extend([(artist_name, lyric_text) for artist_name in row if artist_name])

    return iter(ret_partition)

def compute_word_count(args):
    artist_name, lyric_text = args
    word_counter = collections.Counter(lyric_text.split(' '))
    word_counts = ['%s %s' % (key, value) for key, value in word_counter.iteritems()]
    word_counts_str = ' '.join(word_counts)
    return artist_name, word_counts_str

def flat_map_to_word_count(args):
    artist_name, word_counts_str = args
    word_counts = word_counts_str.split(' ')
    ret = []

    for i in xrange(0, len(word_counts), 2):
        word, count = word_counts[i], word_counts[i+1]
        ret.append((word, (artist_name, count)))
    return ret

def word_count_map_to_tfidf(args):
    word, artist, count, total_count = args[0], args[1][0][0], args[1][0][1], args[1][1]
    tfidf_factor, threshold = 1000000, 5
    tfidf = 0.0 if int(count) < threshold else float(count) / float(total_count) * tfidf_factor
    return (artist, '%s %s' % (word, tfidf))

def bulk_insert_words_to_artists_count(partition, trivial_words, column_prefix, table_name):
    batch = happybase.Connection(constants.MASTER_HOST, constants.HBASE_PORT).table(table_name).batch(batch_size = 1000)

    for artist_name, word_counts_str in partition:
        word_counts = word_counts_str.split(' ')
        count_data = {}

        for i in xrange(0, len(word_counts), 2):
            word, count = word_counts[i], word_counts[i+1]
            count_data['%s:%s' % (column_prefix, word)] = count

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

        batch.put(artist_name, count_data)

def bulk_insert_words_to_words_count(partition):
    connection = happybase.Connection(constants.MASTER_HOST, constants.HBASE_PORT)
    batch = connection.table(constants.WORDS_COUNT_TABLE).batch(batch_size = 1000)
    for word, count in partition:
        data = { 'counts:count' : str(count) }
        batch.put(word, data)

def main():
    conf = pyspark.conf.SparkConf()
    conf.setAppName(APP_NAME)
    sc = pyspark.context.SparkContext(conf=conf)

    remover = pyspark.ml.feature.StopWordsRemover()
    trivial_words = set(remover.loadDefaultStopWords('english'))
    shared_trivial_words = sc.broadcast(trivial_words)

    try:
        words_count_rdd = sc.textFile('hdfs://%s:%s/resources/norm_words' % (constants.MASTER_HOST, constants.HDFS_PORT)) \
                            .map(lambda line: eval(line))
        words_count_rdd.take(1)
    except Exception, err:
        words_count_rdd = sc.textFile('hdfs://%s:%s/resources/raw_data/raw_lyrics.txt' % (constants.MASTER_HOST, constants.HDFS_PORT)) \
                            .flatMap(flat_map_words) \
                            .reduceByKey(lambda a, b: a + b)
        words_count_rdd.saveAsTextFile('hdfs://%s:%s/resources/norm_words' % (constants.MASTER_HOST, constants.HDFS_PORT))
        words_count_rdd.foreachPartition(bulk_insert_words_to_words_count)

    try:
        lyrics_to_words_rdd = sc.textFile('hdfs://%s:%s/resources/norm_lyrics' % (constants.MASTER_HOST, constants.HDFS_PORT)) \
                                .map(lambda line: eval(line))
        lyrics_to_words_rdd.take(1)
    except Exception, err:
        lyrics_to_words_rdd = sc.textFile('hdfs://%s:%s/resources/raw_data/raw_lyrics.txt' % (constants.MASTER_HOST, constants.HDFS_PORT)) \
                                .filter(is_valid_record) \
                                .map(load_and_extract) \
                                .mapPartitions(map_lyricid_to_artistname) \
                                .reduceByKey(lambda a, b: '%s %s' % (a, b)) \
                                .map(compute_word_count)
        lyrics_to_words_rdd.saveAsTextFile('hdfs://%s:%s/resources/norm_lyrics' % (constants.MASTER_HOST, constants.HDFS_PORT))
        lyrics_to_words_rdd.foreachPartition(lambda partition: bulk_insert_words_to_artists_count(partition, shared_trivial_words.value, 'words', constants.ARTISTS_WORDS_COUNT_TABLE))

    try:
        tfidf_rdd = sc.textFile('hdfs://%s:%s/resources/norm_tfidf' % (constants.MASTER_HOST, constants.HDFS_PORT)) \
                      .map(lambda line: eval(line))
        tfidf_rdd.take(1)
    except Exception, err:
        tfidf_rdd = lyrics_to_words_rdd.flatMap(flat_map_to_word_count) \
                                       .join(words_count_rdd) \
                                       .map(word_count_map_to_tfidf) \
                                       .reduceByKey(lambda a, b: '%s %s' % (a, b))
        tfidf_rdd.saveAsTextFile('hdfs://%s:%s/resources/norm_tfidf' % (constants.MASTER_HOST, constants.HDFS_PORT))
        tfidf_rdd.foreachPartition(lambda partition: bulk_insert_words_to_artists_count(partition, shared_trivial_words.value, 'words_tf_idf', constants.ARTISTS_WORDS_TFIDF_TABLE))

if __name__ == '__main__':
    main()
