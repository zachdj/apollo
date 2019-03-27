"""
Flask server for handling HTTP queries. 

Both queries for static files and queries to the solar farm database are handled. 
    
usage: solarserver.py [-h] [--host IP] [--port N] [--html HTML_DIR]
                      [--schemas SCHEMAS_DIR] [--dbdir DB_DIR]
                      [--dbfile DB_FILE] [--dburl dburl] [--htmlurl htmlurl]
                      [--log LOG]

arguments:
  -h, --help            show this help message and exit
  --host IP             The IP to listen on. Default is 127.0.0.1.
  --port N              The port the server should listen on. Default is 5000.
  --html HTML_DIR       The directory for html and other static files to be
                        served. 
  --schemas SCHEMAS_DIR
                        The directory storing the JSON description of the
                        database schema. Default can be set in cfg.py.
  --dbdir DB_DIR        The directory storing the sqlite database(s) to use.
                        Default can be set in cfg.py.
  --dbfile DB_FILE      The default database file to use. Default can be set
                        in cfg.py.
  --dburl dburl         The URL to bind to database queries.
  --htmlurl htmlurl     The URL to bind to static (html) queries.
  --log LOG             Sets the log level. One of INFO, DEBUG, ERROR, etc.
                        Default is INFO
    
EXAMPLE: 
>python -m apollo.server.solarserver --host 127.0.0.1 --port 5000 --html "I:\html" --schemas "I:\schemas" --dbdir "I:\db" --dbfile "solar_farm.db" --log DEBUG

[2019-03-21 17:33:46,870] INFO:  * host:127.0.0.1
[2019-03-21 17:33:46,870] INFO:  * port:5000
[2019-03-21 17:33:46,870] INFO:  * html:I:\html
[2019-03-21 17:33:46,871] INFO:  * html url:/html
[2019-03-21 17:33:46,872] INFO:  * schema dir:I:\schemas
[2019-03-21 17:33:46,873] INFO:  * dbdir:I:\db
[2019-03-21 17:33:46,873] INFO:  * dbfile:solar_farm.db
[2019-03-21 17:33:46,875] INFO:  * db url:/solar
[2019-03-21 17:33:47,206] INFO:  * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
"""
from flask import Flask
from flask import request
from flask import jsonify
from flask import send_from_directory
import apollo.server.handlers as handlers
import apollo.server.cfg as cfg
import apollo.db.dbapi as dbapi
import os
import argparse
import webbrowser
import threading
import logging
from pathlib import Path
import traceback
from waitress import serve

logger = logging.getLogger(__name__)

##################################################################
# CONSTANTS
##################################################################

TIMESTAMP = cfg.TIMESTAMP

app = Flask(__name__)

def handle_bad_request(e):
    logger.error(str(e))
    traceback.print_exc()
    return 'Bad Request: '+str(e), 400

app.register_error_handler(400, handle_bad_request)

#############################################
#############################################
#@app.route("/solar")
def solar_query():
    try:
        groupby =  request.args.get(cfg.QUERY_GROUPBY_KEY, "")
        if groupby in cfg.USE_PROC:
            handler = handlers.SolarDBRequestHandlerPostProcessing()
        else:
            handler = handlers.SolarDBRequestHandler()
        return handler.handle_request(request, dbfile=str(cfg.DB_FILE))
    except Exception as e:
        return handle_bad_request(e)

#############################################
#############################################
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
        source = request.args.get("source",None)
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
        source = request.args.get("source",None)
        table = request.args.get("table",None)
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

def config_from_args():
    parser = argparse.ArgumentParser(description="""Starts a Flask server to handle HTTP requests to the solar farm database. 
                                     EXAMPLE: 
>python -m apollo.server.solarserver --host 127.0.0.1 --port 5000 --html "I:\html" --schemas "I:\schemas" --dbdir "I:\db" --dbfile "solar_farm.db" --log DEBUG

                                     Static files are served from the specified HTML directory.""")
    parser.add_argument('--host', metavar='IP', type=str, nargs=1,dest='host',default='127.0.0.1',help='The IP to listen on. Default is 127.0.0.1.')
    parser.add_argument('--port', metavar='N', type=int, nargs=1,dest='port',default=5000,help='The port the server should listen on. Default is 5000.')
    parser.add_argument('--html', metavar='HTML_DIR', type=str, nargs=1,dest='html',default='',help='The directory for html and other static files to be served. Default can be set in cfg.py.')
    parser.add_argument('--schemas', metavar='SCHEMAS_DIR', type=str, nargs=1,dest='schemas',default='',help='The directory storing the JSON description of the database schema. Default can be set in cfg.py.')
    parser.add_argument('--dbdir', metavar='DB_DIR', type=str, nargs=1,dest='db_dir',default='',help='The directory storing the sqlite database(s) to use. Default can be set in cfg.py.')
    parser.add_argument('--dbfile', metavar='DB_FILE', type=str, nargs=1,dest='db_file',default='',help='The default database file to use. Default can be set in cfg.py.')
    parser.add_argument('--dburl', metavar='dburl', type=str, nargs=1,dest='dburl',default='/solar',help='The URL to bind to database queries.')
    parser.add_argument('--htmlurl', metavar='htmlurl', type=str, nargs=1,dest='htmlurl',default='/html',help='The URL to bind to static (html) queries.')
    parser.add_argument('--log', type=str, default='INFO', help='Sets the log level. One of INFO, DEBUG, ERROR, etc. Default is INFO')
    
    args = parser.parse_args()

    logging.basicConfig(format='[{asctime}] {levelname}: {message}', style='{', level=args.log)
    logger.setLevel(args.log)

    if isinstance(args.host,list):
        args.host = args.host[0]
    if isinstance(args.port,list):
        args.port = args.port[0]
    if isinstance(args.html,list):
        args.html = args.html[0]
    if isinstance(args.schemas,list):
        args.schemas = args.schemas[0]
    if isinstance(args.db_dir,list):
        args.db_dir = args.db_dir[0]
    if isinstance(args.db_file,list):
        args.db_file = args.db_file[0]
        
    if isinstance(args.dburl,list):
        args.dburl = args.dburl[0]
    if isinstance(args.htmlurl,list):
        args.htmlurl = args.htmlurl[0]

    if args.html == '':
        args.html = cfg.HTML_DIR
    else:
        cfg.HTML_DIR = parsePath(args.html)

    if args.schemas == '':
        args.schemas = cfg.SCHEMA_DIR
    else:
        cfg.SCHEMA_DIR = parsePath(args.schemas)

    if args.db_dir == '':
        args.db_dir = cfg.DB_DIR
    else:
        cfg.DB_DIR = parsePath(args.db_dir)
        
    if args.db_file == '':
        args.db_file = cfg.DB_FILE
    else:
        cfg.DB_FILE = cfg.DB_DIR / args.db_file

    logging.info(" * host:"+str(args.host))
    logging.info(" * port:"+str(args.port))
    logging.info(" * html:"+str(args.html))
    logging.info(" * html url:"+str( args.htmlurl))
    logging.info(" * schema dir:"+str(args.schemas))
    logging.info(" * dbdir:"+str( args.db_dir))
    logging.info(" * dbfile:"+str( args.db_file))
    logging.info(" * db url:"+str( args.dburl))
    
    return args

if __name__ == "__main__":
    args = config_from_args()
    try:
        threading.Timer(4, lambda: webbrowser.open("http://"+ args.host+":"+str(args.port)+"/html/solar/start.html", new=2)).start()
    except:
        pass
    app.add_url_rule(args.dburl, 'solar_query', solar_query)
    app.add_url_rule(args.htmlurl+"/<path:path>", 'send_files', send_files)
    
    # Use when Flask is the primary server. 
    #app.run(debug=False, host=args.host, port=args.port)
    
    # Use Waitress instead. Use host='0.0.0.0' to make it public. 
    serve(app, host='127.0.0.1', port=5000) 

#############################################
#############################################
