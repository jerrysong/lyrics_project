HADOOP_MASTER_HOST = '192.155.208.14'
HDFS_PORT = 8020

HBASE_THRIFT_HOST = '50.23.83.252'
HBASE_PORT = 9100

WEB_APP_HOST = '50.23.83.252'
WEB_APP_PORT = 80

RAW_LYRICS_HDFS_PATH = 'hdfs://%s:%s/resources/raw_data/raw_lyrics.txt' % (HADOOP_MASTER_HOST, HDFS_PORT)
RAW_ARTISTS_HDFS_PATH = 'hdfs://%s:%s/resources/raw_data/raw_artists.txt' % (HADOOP_MASTER_HOST, HDFS_PORT)

LYRICS_TO_ARTISTS_TABLE = 'lyrics_to_artists'

SINGLE_WORD_ARTISTS_WORDS_COUNT_TABLE = 'single_word_artists_to_words_count'
SINGLE_WORD_CORPUS_WORDS_COUNT_TABLE = 'single_word_corpus_words_count'
SINGLE_WORD_MOST_FREQUENT_WORDS_TABLE = 'single_word_most_frequent_words'
SINGLE_WORD_CORPUS_WORDS_COUNT_HDFS_PATH = 'hdfs://%s:%s/resources/norm_data/corpus_word_count/single_word' % (HADOOP_MASTER_HOST, HDFS_PORT)
SINGLE_WORD_NORM_ARTISTS_WORDS_COUNT_HDFS_PATH = 'hdfs://%s:%s/resources/norm_data/artist_word_count/single_word' % (HADOOP_MASTER_HOST, HDFS_PORT)
SINGLE_WORD_NORM_ARTISTS_WORDS_TFIDF_HDFS_PATH = 'hdfs://%s:%s/resources/norm_data/artist_word_tfidf/single_word' % (HADOOP_MASTER_HOST, HDFS_PORT)

TWO_GRAM_ARTISTS_WORDS_COUNT_TABLE = 'two_gram_artists_to_words_count'
TWO_GRAM_CORPUS_WORDS_COUNT_TABLE = 'two_gram_corpus_words_count'
TWO_GRAM_MOST_FREQUENT_WORDS_TABLE = 'two_gram_word_most_frequent_words'
TWO_GRAM_CORPUS_WORDS_COUNT_HDFS_PATH = 'hdfs://%s:%s/resources/norm_data/corpus_word_count/two_gram_word' % (HADOOP_MASTER_HOST, HDFS_PORT)
TWO_GRAM_NORM_ARTISTS_WORDS_COUNT_HDFS_PATH = 'hdfs://%s:%s/resources/norm_data/artist_word_count/two_gram_word' % (HADOOP_MASTER_HOST, HDFS_PORT)
TWO_GRAM_NORM_ARTISTS_WORDS_TFIDF_HDFS_PATH = 'hdfs://%s:%s/resources/norm_data/artist_word_tfidf/two_gram_word' % (HADOOP_MASTER_HOST, HDFS_PORT)
