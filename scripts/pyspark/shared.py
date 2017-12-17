import constants
import happybase
import heapq

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

def fetch_all_row_keys(table):
    """ Fetch all row keys of a given Hbase table.
    """
    connection = happybase.Connection(constants.HBASE_THRIFT_HOST, constants.HBASE_PORT)
    t = connection.table(table)
    return set([row[0] for row in t.scan()])

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
    batch = happybase.Connection(constants.HBASE_THRIFT_HOST, constants.HBASE_PORT) \
                     .table(table_name).batch(batch_size = 1000)

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
