import constants
import flask
import json
import os
import hbase_manager
import werkzeug

class Server(flask.Flask):

    def __init__(self, import_name):
        super(Server, self).__init__(import_name)
        self.hbase = hbase_manager.HBaseManager()

app = Server('Lyrics Web App')

@app.route('/')
def hello():
    return flask.render_template('index.html')

@app.route('/static/<path:path>')
def send_static(path):
    split_path = path.split('/')
    path, filename = os.path.join(*split_path[:-1]), split_path[-1]
    return flask.send_from_directory(path, filename)

@app.route('/get_top_words_by_artist', methods=['POST'])
def get_top_words_by_artist():
    artist = flask.request.get_json(force=True).get('artist')
    if not artist:
        return flask.jsonify({})

    artist = artist.lower()
    top_10_by_cnt = app.hbase.get_top_10_by_cnt_by_artist_name(artist)
    top_10_by_tfidf = app.hbase.get_top_10_by_tfidf_by_artist_name(artist)
    ret_json = {
        'top_10_by_cnt': dict(top_10_by_cnt),
        'top_10_by_tfidf': dict(top_10_by_tfidf)
    }
    return flask.jsonify(ret_json)

if __name__ == '__main__':
    app.run(host=constants.MASTER_HOST, port=constants.WEB_APP_PORT)
