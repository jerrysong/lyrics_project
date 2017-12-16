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
        hbase.get_artists_to_word_count_table(constants.SINGLE_WORD_ARTISTS_WORDS_COUNT_TABLE)
        hbase.get_word_count_table(constants.SINGLE_WORD_CORPUS_WORDS_COUNT_TABLE)

    def create_two_gram_count_table(self):
        hbase.get_artists_to_word_count_table(constants.TWO_GRAM_ARTISTS_WORDS_COUNT_TABLE)
        hbase.get_word_count_table(constants.TWO_GRAM_CORPUS_WORDS_COUNT_TABLE)

    def create_three_gram_count_table(self):
        hbase.get_artists_to_word_count_table(constants.THREE_GRAM_ARTISTS_WORDS_COUNT_TABLE)
        hbase.get_word_count_table(constants.THREE_GRAM_CORPUS_WORDS_COUNT_TABLE)

    def get_top_10_by_cnt_by_artist_name(self, name):
        artist = 'artists:' + name
        table = self.get_artists_to_word_count_table('artists_to_words_count')
        ret = table.row(artist, ('top_10_nontrival_words',))
        ret = { key.split(':', 1)[1]:value for key, value in ret.iteritems() }
        return ret

    def get_top_10_by_tfidf_by_artist_name(self, name):
        artist = 'artists:' + name
        table = self.get_artists_to_word_count_table('artists_to_words_count')
        ret = table.row(artist, ('top_10_nontrival_words_tf_idf',))
        ret = { key.split(':', 1)[1]:value for key, value in ret.iteritems() }
        return ret

if __name__ == "__main__":
    hbase = HBaseManager()
    hbase.get_lyrics_to_artists_table()
    hbase.create_word_count_tables()
    hbase.create_two_gram_count_table()
    hbase.create_three_gram_count_table()
