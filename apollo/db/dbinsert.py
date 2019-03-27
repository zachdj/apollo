"""Insert csv files into the solar farm SQLite database.


This script can be used to insert either gzipped log files or else csv files downloaded from 
the REAPR website into an SQLite database of matching format. Either a single file or a directory of files can be inserted. 

The two log formats are slightly different. Files downloaded from the REAPR site have headers, while the gzipped log files do not. 
Also, the gzipped files have 3 additional code columns immediately after the timestamp (the first column). The script can infer the appropriate table from the csv headers of REAPR files, but it table must be specified for the gzipped log files. 

It is possible to indicate that columns for YEAR, MONTH, DAY, HOUR, MINUTE, and DAYOFYEAR should be added to the csv data 
before insertion into the database (this is required to match the database schema). The script does not do this by default.

By default, a single input file to process is expected. However, an entire directory can also be specified. 
"""

import argparse
import apollo.db.dbapi as dbapi
from pathlib import Path
import os
import logging

def process_reapr(db_file,file_dir, table = None, convert=True):
    """Process a directory (of files in REAPR format), inserting its files into the appropriate database table. 
    
    When adding a file, it is converted from csv file into a Pandas data frame.
    Some columns are transformed and others are added. 
    The dataframe is first inserted into a temporary database table and then 
    copied to the final module table (the temporary table is then purged). 
    The temporary table is needed to avoid duplicates throwing errors. 
    (df.to_sql() does not appear to allow automatic 
    ignoring of duplicates). 
    
     Args:
         db_file (str or Path):
             The database to insert into
         file_dir (str or Path):
             The directory of input files to process
         table (str):
             The name of the table to insert into.
         convert (boolean):
             Indicates whether the input should be preprocessed before insertion. 
    """
    
    counter = 0                
    for root, dirs, files in os.walk(file_dir):
        for filename in files:
            infile = Path(root)/filename
            logging.info(f"processing: {infile}")
            try:
                rbh = dbapi.REAPRWrapper()
                rbh.insert_csv(db_file, infile, table=table, convert=convert)
                logging.info(f"Finished: {infile}")
            except Exception as e:
                logging.ERROR(f"Error processing REAPR file {infile}"+str(e))
            counter = counter + 1
            if counter % 10 == 0:
                logging.info(f"files processed: {counter}")


# ### Process REAPR Files
def process_gz(db_file,file_dir, table = None, convert=True):
    """Process a directory (of files in csv or gzipped log format), inserting its files into the appropriate database table. 
    
    When adding a file, it is converted from csv file into a Pandas data frame.
    Some columns are transformed and others are added. 
    The dataframe is first inserted into a temporary database table and then 
    copied to the final module table (the temporary table is then purged). 
    The temporary table is needed to avoid duplicates throwing errors. 
    (df.to_sql() does not appear to allow automatic 
    ignoring of duplicates). 
    
      Args:
         db_file (str or Path):
             The database to insert into
         file_dir (str or Path):
             The directory of input files to process
         table (str):
             The name of the table to insert into.
         convert (boolean):
             Indicates whether the input should be preprocessed before insertion. 
    """

    
    counter = 0                
    for root, dirs, files in os.walk(file_dir):
        for filename in files:
            infile = Path(root)/filename
            logging.info(f"processing: {infile}")
            try:
                rbh = dbapi.SolarLogWrapper()
                rbh.insert_csv(db_file, infile, table=table, convert=convert)
                logging.info(f"Finished: {infile}")
            except Exception as e:
                logging.ERROR(f"Error processing LOG file {infile}"+str(e))
            counter = counter + 1
            if counter % 10 == 0:
                logging.info(f"files processed: {counter}")
    

def _config_from_args():
    parser = argparse.ArgumentParser(description="Utility function for inserting logged data from the solar farm into an SQLite databas. "\
                                     +"Input is a csv file in either the REAPR or gzipped log format. "\
                                     +"The format should already match the format of the destination table (see documentation). "\
                                     +"Alternatively, the raw input can be converted to add year, month, day, hour, minute, and dayofyear columns. ")
    
    parser.add_argument('-d', '--dir',      action='store_true',help='process a directory of input files rather than a single file.')
    parser.add_argument('-c', '--convert',  action='store_true',help='before insertion, convert the input file, adding columns for year, month, day, hour, and minute.')
    parser.add_argument('-u', '--usetemp',  action='store_false',help='insert the data into a temporary database table and then copy it to the final destination table (this is done to work around a Pandas issue).')
    parser.add_argument('-f', '--format',   metavar='format', type=str, choices=['log', 'reapr'], nargs=1,dest='format', help='the format of the input file (gz "log" or "reapr" file).')
    parser.add_argument('-i', '--in',       metavar='in', type=str, nargs=1, dest='infile',default=None,required=True,help='the file (or directory) to insert into the database.')
    parser.add_argument('-b', '--db',       metavar='db', type=str, nargs=1,dest='dbfile',default=None,required=True,help='the target database file to insert into.')
    parser.add_argument('-t', '--table',    metavar='table', type=str, nargs=1,dest='table',default=None,help='the database table to insert values into.')
    parser.add_argument('--log', type=str, default='INFO', help='Sets the log level. One of INFO, DEBUG, ERROR, etc. Default is INFO')
    
    args = parser.parse_args()

    logging.basicConfig(format='[{asctime}] {levelname}: {message}', style='{', level=args.log)

    dbapi.logger.setLevel(args.log)
    
    if isinstance(args.format,list):
        args.format = args.format[0]
    if isinstance(args.infile,list):
        args.infile = args.infile[0]
    if isinstance(args.dbfile,list):
        args.dbfile = args.dbfile[0]
    if isinstance(args.table,list):
        args.table = args.table[0]
    if args.convert and not args.format in ['log', 'reapr']:
        parser.error ("if --convert is specified, then --format must be 'log' or 'reapr' .")
    
    logging.info("database insert...")
    if args.format:
        logging.info(" * format:"+ str(args.format))
    logging.info(" * in:" + str(args.infile))
    logging.info(" * db:" + str(args.dbfile))
    logging.info(" * table:"+ str(args.table))
    logging.info(" * use dir:"+ str(args.dir))
    logging.info(" * convert:"+ str(args.convert))
    logging.info(" * use temp:"+ str(args.usetemp))
    return args

if __name__ == "__main__":
    args = _config_from_args()
    
    handler = dbapi.REAPRWrapper()
    dbfile = str(Path(args.dbfile))
    if args.convert:
        if args.format == 'log':
            handler = dbapi.SolarLogWrapper()
        else:
            handler = dbapi.REAPRWrapper()
        
    if args.dir:
        if args.format == 'log':
            process_gz(dbfile, Path(args.infile), table=args.table,  convert=args.convert)
        else:
            process_reapr(dbfile, Path(args.infile), table=args.table,  convert=args.convert)
    else:
        handler.insert_csv(dbfile, args.infile, table=args.table,usetemp=True, convert=args.convert)
        
    