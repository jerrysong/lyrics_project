import constants
import flask
import json
import os
import hbase_manager

class Server(flask.Flask):

    def __init__(self, import_name):
        super(Server, self).__init__(import_name)
        self.hbase = hbase_manager.HBaseManager()

app = Server('Lyrics Web App')

@app.route('/')
def hello():
    return flask.render_template('index.html')

@app.route('/<path:path>')
def send_static(path):
    split_path = path.split('/')
    path, filename = os.path.join(*split_path[:-1]), split_path[-1]
    return flask.send_from_directory(os.path.join('static', path), filename)

@app.route('/check', methods=['POST'])
def check_artists():
    artist = flask.request.get_json(force=True).get('artist')
    if not artist:
        return ''

    artist = artist.lower()
    res = app.hbase.get_top_10_words_by_artist_name(artist)
    sorted_res = sorted(res.iteritems(), key=lambda t:int(t[1]), reverse=True)
    return str(sorted_res)

if __name__ == '__main__':
    app.run(host=constants.MASTER_HOST, port=constants.WEB_APP_PORT)
