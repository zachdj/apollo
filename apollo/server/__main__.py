""" Flask server for handling HTTP queries.

Both queries for static files and queries to the solar farm database are handled
    
usage: python -m apollo.server [-h] [--host IP] [--port N] [--html HTML_DIR]
                      [--schemas SCHEMAS_DIR] [--dbdir DB_DIR]
                      [--dbfile DB_FILE] [--dburl dburl] [--htmlurl htmlurl]
                      [--log LOG]

"""
import argparse
from flask import Flask
from flask import request
from flask import jsonify
from flask import send_from_directory
import logging
import os
from pathlib import Path
import threading
import traceback
from waitress import serve
import webbrowser

import apollo.db.dbapi as dbapi
import apollo.server.handlers as handlers
import apollo.server.cfg as cfg
import apollo.storage as storage

logger = logging.getLogger(__name__)

app = Flask(__name__)


def handle_bad_request(e):
    logger.error(str(e))
    traceback.print_exc()
    return f'Bad Request: {e}', 400


app.register_error_handler(400, handle_bad_request)


#@app.route("/solar")
def solar_query():
    try:
        groupby = request.args.get(cfg.QUERY_GROUPBY_KEY, "")
        if groupby in cfg.USE_PROC:
            handler = handlers.SolarDBRequestHandlerPostProcessing()
        else:
            handler = handlers.SolarDBRequestHandler()
        return handler.handle_request(request, dbfile=str(cfg.DB_FILE))
    except Exception as e:
        return handle_bad_request(e)


@app.route("/status")
def get_status():
    try:
        return jsonify({"status":1})
    except Exception as e:
        return handle_bad_request(e)


#@app.route('/html/<path:path>')
def send_files(path):
    return send_from_directory(cfg.HTML_DIR, path)


@app.route('/tables')
def get_tables():
    dbh = None
    try:
        source = request.args.get('source', None)
        dbh = dbapi.DBHandler(cfg.DB_DIR/source)
        dbh.connect()
        tables = dbh.tables()
        dbh.close()
        return jsonify(tables)
    except Exception as e:
        if dbh:
            dbh.close()
        return handle_bad_request(e)


@app.route('/sources')
def get_sources():
    try:
        sources = [f for f in os.listdir(cfg.DB_DIR) if f.endswith(".db")]
        return jsonify(sources)
    except Exception as e:
        return handle_bad_request(e)


@app.route('/columns')
def get_columns():
    dbh = None
    try:
        source = request.args.get("source", None)
        table = request.args.get("table", None)
        dbh = dbapi.DBHandler(cfg.DB_DIR/source)
        dbh.connect()
        columns = dbh.column_names(table)
        dbh.close()
        return jsonify(columns)
    except Exception as e:
        if dbh:
            dbh.close()
        return handle_bad_request(e)


def parsePath(inPath):
    return Path(inPath.replace('"','').replace("'",""))


def main():
    parser = argparse.ArgumentParser(
        description="Starts a Flask server to handle HTTP requests to the "
                    "solar farm database. ")

    parser.add_argument(
        '--host', metavar='IP', dest='host',
        type=str, default='127.0.0.1',
        help='The IP to listen on.')
    parser.add_argument(
        '--port', metavar='PORT', dest='port',
        type=int, default=5000,
        help='The port the server should listen on.')
    parser.add_argument(
        '--html', metavar='HTML_DIR', dest='html',
        type=str, default=storage.get('assets/html'),
        help='The directory for html and other static files to be served.')
    parser.add_argument(
        '--schemas', metavar='SCHEMAS_DIR', dest='schemas',
        type=str, default=storage.get('assets/schemas'),
        help='The directory storing the JSON description '
             'of the database schema.')
    parser.add_argument(
        '--dbdir', metavar='DB_DIR', dest='db_dir',
        type=str, default=storage.get('GA-POWER'),
        help='The directory storing the sqlite database(s) to use.')
    parser.add_argument(
        '--dbfile', metavar='DB_FILE', dest='db_file',
        type=str, default='solar_farm.sqlite',
        help='The default database file to use.')
    parser.add_argument(
        '--dburl', metavar='dburl', dest='dburl',
        type=str, default='/solar',
        help='The URL to bind to database queries.')
    parser.add_argument(
        '--htmlurl', metavar='htmlurl', dest='htmlurl',
        type=str, default='/html',
        help='The URL to bind to static (html) queries.')
    parser.add_argument(
        '--log', type=str, default='INFO',
        choices=('INFO', 'DEBUG', 'WARN', 'ERROR'),
        help='Sets the log level.')

    args = parser.parse_args()
    logging.basicConfig(format='[{asctime}] {levelname}: {message}',
                        style='{', level=args.log)
    logger.setLevel(args.log)

    logging.info(f'Starting Apollo server with config:\n{vars(args)}')

    cfg.HTML_DIR = parsePath(str(args.html))
    cfg.SCHEMA_DIR = parsePath(str(args.schemas))
    cfg.DB_DIR = parsePath(str(args.db_dir))
    cfg.DB_FILE = cfg.DB_DIR / args.db_file

    app.add_url_rule(args.dburl, 'solar_query', solar_query)
    app.add_url_rule(args.htmlurl + "/<path:path>", 'send_files', send_files)

    index_url = f'http://{args.host}:{args.port}/html/solar/start.html'
    try:
        threading.Timer(4, lambda: webbrowser.open(index_url, new=2)).start()
    except:
        pass
    serve(app, host='127.0.0.1', port=5000)


if __name__ == '__main__':
    main()
