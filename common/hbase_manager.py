import constants
import json
import happybase
import os

class HBaseManager(object):

    def __init__(self):
        self.host = constants.HBASE_THRIFT_HOST
        self.port = constants.HBASE_PORT

    def get_lyrics_to_artists_table(self):
        connection = happybase.Connection(self.host, self.port)
        if not constants.LYRICS_TO_ARTISTS_TABLE in connection.tables():
            families = {'artists': dict()}
            connection.create_table(constants.LYRICS_TO_ARTISTS_TABLE, families)
        return connection.table(constants.LYRICS_TO_ARTISTS_TABLE)

    def get_artists_to_word_count_table(self, table):
        connection = happybase.Connection(self.host, self.port)
        if not table in connection.tables():
            families = {
                'words': dict(),
                'top_10_words': dict(),
                'top_10_nontrival_words': dict(),
                'words_tf_idf': dict(),
                'top_10_words_tf_idf': dict(),
                'top_10_nontrival_words_tf_idf': dict()
            }
            connection.create_table(table, families)
        return connection.table(table)

    def get_word_count_table(self, table):
        connection = happybase.Connection(self.host, self.port)
        if not table in connection.tables():
            families = {
                'counts': dict(),
            }
            connection.create_table(table, families)
        return connection.table(table)

    def create_word_count_tables(self):
        self.get_artists_to_word_count_table(constants.SINGLE_WORD_ARTISTS_WORDS_COUNT_TABLE)
        self.get_word_count_table(constants.SINGLE_WORD_CORPUS_WORDS_COUNT_TABLE)
        self.get_word_count_table(constants.SINGLE_WORD_MOST_FREQUENT_WORDS_TABLE)

    def create_two_gram_count_table(self):
        self.get_artists_to_word_count_table(constants.TWO_GRAM_ARTISTS_WORDS_COUNT_TABLE)
        self.get_word_count_table(constants.TWO_GRAM_CORPUS_WORDS_COUNT_TABLE)
        self.get_word_count_table(constants.TWO_GRAM_MOST_FREQUENT_WORDS_TABLE)

    def get_top_10_by_cnt_by_artist_name(self, name):
        artist = 'artists:' + name

        single_word_table = self.get_artists_to_word_count_table(constants.SINGLE_WORD_ARTISTS_WORDS_COUNT_TABLE)
        top_10_nontrival_words = single_word_table.row(artist, ('top_10_nontrival_words',))
        top_10_nontrival_words =  self.normalize_count_row(top_10_nontrival_words)
        top_10_nontrival_words_tf_idf = single_word_table.row(artist, ('top_10_nontrival_words_tf_idf',))
        top_10_nontrival_words_tf_idf = self.normalize_count_row(top_10_nontrival_words_tf_idf)

        two_gram_word_table = self.get_artists_to_word_count_table(constants.TWO_GRAM_ARTISTS_WORDS_COUNT_TABLE)
        top_10_nontrival_two_gram_words = two_gram_word_table.row(artist, ('top_10_nontrival_words',))
        top_10_nontrival_two_gram_words = self.normalize_count_row(top_10_nontrival_two_gram_words)
        top_10_nontrival_two_gram_words_tf_idf = two_gram_word_table.row(artist, ('top_10_nontrival_words_tf_idf',))
        top_10_nontrival_two_gram_words_tf_idf = self.normalize_count_row(top_10_nontrival_two_gram_words_tf_idf)

        ret = {
            'top_single_word': top_10_nontrival_words,
            'top_single_word_tfidf': top_10_nontrival_words_tf_idf,
            'top_two_gram_word': top_10_nontrival_two_gram_words,
            'top_two_gram_word_tfidf': top_10_nontrival_two_gram_words_tf_idf
        }

        return ret

    def normalize_count_row(self, row):
        return [{'text': key.split(':', 1)[1], 'value': int(float(value))} for key, value in row.iteritems()]

if __name__ == "__main__":
    hbase = HBaseManager()
    hbase.get_lyrics_to_artists_table()
    hbase.create_word_count_tables()
    hbase.create_two_gram_count_table()
