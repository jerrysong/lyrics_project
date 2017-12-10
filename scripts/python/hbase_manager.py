import json
import happybase
import os

PROD_PATH = os.environ.get('PROD')
CLUSTER_CONFIG_PATH = PROD_PATH + '/conf/cluster_conf.json'

LYRICS_TO_ARTISTS_TABLE = 'lyrics_to_artists'
WORDS_COUNT_TABLE = 'artists_to_words_count'
TWO_GRAMS_COUNT_TABLE = 'two_grams_count'
THREE_GRAMS_COUNT_TABLE = 'three_grams_count'

class HBaseManager(object):

    def __init__(self):
        cluster_config = json.loads(open(CLUSTER_CONFIG_PATH).read())
        host = cluster_config['masterHost']
        port = cluster_config['hbaseThriftPort']
        self.connection = happybase.Connection(host, port)
        self.create_lyrics_to_artists_table_if_not_exist()
        self.create_artists_to_word_count_table_if_not_exist()

    def create_lyrics_to_artists_table_if_not_exist(self):
        if not LYRICS_TO_ARTISTS_TABLE in self.connection.tables():
            families = {'artists': dict()}
            self.connection.create_table(LYRICS_TO_ARTISTS_TABLE, families)
        return self.connection.table(LYRICS_TO_ARTISTS_TABLE)

    def create_artists_to_word_count_table_if_not_exist(self):
        if not WORDS_COUNT_TABLE in self.connection.tables():
            families = {
                'words': dict(),
                'top_10_words': dict()
            }
            self.connection.create_table(WORDS_COUNT_TABLE, families)
        return self.connection.table(WORDS_COUNT_TABLE)

if __name__ == "__main__":
    hbase = HBaseManager()
    hbase.create_lyrics_to_artists_table_if_not_exist()
    hbase.create_artists_to_word_count_table_if_not_exist()
