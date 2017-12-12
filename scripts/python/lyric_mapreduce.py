import collections
import constants
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
    connection = happybase.Connection(MASTER_HOST, HBASE_PORT)
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

def bulk_insert_words_to_artists_count(partition, trivial_words):
    batch = happybase.Connection(MASTER_HOST, HBASE_PORT).table(constants.ARTISTS_WORDS_COUNT_TABLE).batch(batch_size = 1000)

    for t in partition:
        if isinstance(t, unicode):
            t = eval(t)
        artist_name, word_counts_str = t[0], t[1]
        word_counts = word_counts_str.split(' ')
        count_data = {}

        for i in xrange(0, len(word_counts), 2):
            word, count = word_counts[i], word_counts[i+1]
            count_data['words:%s' % (word,)] = count

        top_words = heapq.nlargest(110, count_data.iteritems(), key=lambda t:int(t[1]))
        for word, count in top_words[:10]:
            count_data['top_10_%s' % (word,)] = count

        cnt = 0
        for word, count in top_words:
            if word[6:] not in trivial_words:
                count_data['top_10_nontrival_%s' % (word,)] = count
                cnt += 1
            if cnt == 10:
                break

        batch.put(artist_name, count_data)

def bulk_insert_words_to_artists_tfidf(partition):
    batch = happybase.Connection(MASTER_HOST, HBASE_PORT).table(constants.ARTISTS_WORDS_TFIDF_TABLE).batch(batch_size = 1000)
    words_cnt_table = happybase.Connection(MASTER_HOST, HBASE_PORT).table(constants.WORDS_COUNT_TABLE)
    for t in partition:
        if isinstance(t, unicode):
            t = eval(t)
        artist_name, word_counts_str = t[0], t[1]
        word_counts = word_counts_str.split(' ')
        tfidf_data = {}

        for i in xrange(0, len(word_counts), 2):
            word, count = word_counts[i], word_counts[i+1]
            if not word:
                continue

            row = words_cnt_table.row(word)
            corpus_count = row.get('counts:count')
            if corpus_count:
                tfidf = float(count) / float(corpus_count) * 1000000
                tfidf_data['words_tf_idf:%s' % (word,)] = int(tfidf)

        top_10_words_tf_idf = heapq.nlargest(10, tfidf_data.iteritems(), key=lambda t:t[1])
        for word, tfidf in top_10_words_tf_idf:
            tfidf_data['top_10_%s' % (word,)] = tfidf

        tfidf_data = {key:str(value) for key, value in tfidf_data.iteritems()}
        batch.put(artist_name, tfidf_data)

def bulk_insert_words(partition):
    connection = happybase.Connection(MASTER_HOST, HBASE_PORT)
    batch = connection.table(constants.WORDS_COUNT_TABLE).batch(batch_size = 1000)
    for word, count in partition:
        data = { 'counts:count' : str(count) }
        batch.put(word, data)

def main():
    conf = pyspark.conf.SparkConf()
    conf.setAppName(APP_NAME)
    sc = pyspark.context.SparkContext(conf=conf)
    trivial_words = set(['the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i', 'it', 'for', 'not', 'on', 'with', 'he', 'as',
                         'you', 'do', 'at', 'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she', 'or', 'an', 'will',
                         'my', 'one', 'all', 'would', 'there', 'their', 'what', 'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which',
                         'go', 'me', 'when', 'make', 'can', 'like', 'time', 'no', 'just', 'him', 'know', 'take', 'people', 'into', 'year',
                         'your', 'good', 'some', 'could', 'them', 'see', 'other', 'than', 'then', 'now', 'look', 'only', 'come', 'its',
                         'over', 'think', 'also', 'back', 'after', 'use', 'two', 'how', 'our', 'work', 'first', 'well', 'way', 'even', 'new',
                         'want', 'because', 'any', 'these', 'give', 'day', 'most', 'us', 'is', 'are', 'were', 'was', 'am'])
    shared_trivial_words = sc.broadcast(trivial_words)

    try:
        words_rdd = sc.textFile('hdfs://%s:%s/resources/norm_words' % (MASTER_HOST, HDFS_PORT))
        words_rdd.take(1)
    except Exception, err:
        words_rdd = sc.textFile('hdfs://%s:%s/resources/raw_data/raw_lyrics.txt' % (MASTER_HOST, HDFS_PORT)) \
                      .flatMap(flat_map_words) \
                      .reduceByKey(lambda a, b: a + b)
        words_rdd.saveAsTextFile('/resources/norm_words')
        words_rdd.foreachPartition(bulk_insert_words)

    try:
        lyrics_to_words_rdd = sc.textFile('hdfs://%s:%s/resources/norm_lyrics' % (MASTER_HOST, HDFS_PORT))
        lyrics_to_words_rdd.take(1)
    except Exception, err:
        lyrics_to_words_rdd = sc.textFile('hdfs://%s:%s/resources/raw_data/raw_lyrics.txt' % (MASTER_HOST, HDFS_PORT)) \
                                .filter(is_valid_record) \
                                .map(load_and_extract) \
                                .mapPartitions(map_lyricid_to_artistname) \
                                .reduceByKey(lambda a, b: '%s %s' % (a, b)) \
                                .map(compute_word_count)
        lyrics_to_words_rdd.saveAsTextFile('/resources/norm_lyrics')

    lyrics_to_words_rdd.foreachPartition(lambda partition: bulk_insert_words_to_artists_count(partition, shared_trivial_words.value))
    #lyrics_to_words_rdd.foreachPartition(bulk_insert_words_to_artists_tfidf)

if __name__ == "__main__":
    main()
