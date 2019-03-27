"""Convert a csv file of solar farm log data. 

This script can be used to convert either gzipped log files or else csv files downloaded from 
the REAPR website for use in an SQLite database of matching format. Either a single file or a directory of files can be converted

Two log formats can be converted--the format of files downloaded from the REAPR website, or those of gzipped log files from the UGA solar farm. The formats  are slightly different. Files downloaded from the REAPR site have headers, while the gzipped log files do not. 
Also, the gzipped files have 3 additional code columns immediately after the timestamp (the first column). 

The conversion process uses the timestamp of the log data (the first column) and adds columns for YEAR, MONTH, DAY, HOUR, MINUTE, and DAYOFYEAR. 
For the REAPR files, it also adds three dummy columns (CODE1, CODE2, CODE3) to match columns found int he gzipped file.

The output of the conversion is intended to be csv file suitable for insertion into a corresponding database. 
 
By default, a single input file to process is expected. However, an entire directory can also be specified. 
"""
import argparse
import apollo.db.dbapi as dbapi
import os
from pathlib import Path
import logging

def ensure_dir_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def config_from_args():
    parser = argparse.ArgumentParser(description="Utility function for converting logged data from the solar farm. "\
                                     +"Input is a csv file (or directory of files) in either the REAPR or gzipped log format. "\
                                     +"The input is converted by standardizing timestamps and adding year, month, day, hour, minute, and dayofyear columns. "\
                                     +"The output files are instended to be later inserted into a database with matching format.")
    
    parser.add_argument('-f', '--format',   metavar='format', type=str, choices=['log', 'reapr'], nargs=1,dest='format', help='the format of the input file (gz "log" or "reapr" file).')
    parser.add_argument('-d', '--dir',      action='store_true',help='process a directory of input files rather than a single file.')
    parser.add_argument('-i', '--in', metavar='in', type=str, nargs=1,dest='infile',default=None,required=True,help='the file (or directory) to convert.')
    parser.add_argument('-o', '--out', metavar='out', type=str, nargs=1,dest='outfile',default=None,required=True,help='the file (or directory of files) resulting from the conversion.')
    parser.add_argument('--log', type=str, default='INFO', help='Sets the log level. One of INFO, DEBUG, ERROR, etc. Default is INFO')
    
    args = parser.parse_args()

    logging.basicConfig(format='[{asctime}] {levelname}: {message}', style='{', level=args.log)
    
    dbapi.logger.setLevel(args.log)
 
    if isinstance(args.format,list):
        args.format = args.format[0]
    if isinstance(args.infile,list):
        args.infile = args.infile[0]
    if isinstance(args.outfile,list):
        args.outfile = args.outfile[0]
    if not args.format in ['log', 'reapr']:
        parser.error ("--format must be 'log' or 'reapr' .")
    logging.info(" * format:" +str(args.format))
    logging.info(" * in:"+str(args.infile))
    logging.info(" * out:"+str(args.outfile))
    logging.info(" * use dir:"+str(args.dir))
    return args

if __name__ == "__main__":
    args = config_from_args()
    handler = None
    if args.format == 'log':
        handler = dbapi.SolarLogWrapper()
    else:
        handler = dbapi.REAPRWrapper()
    if args.dir:
        outdir = Path(args.outfile)
        ensure_dir_exists(outdir)
        handler.convert_dir(Path(args.infile), outdir)
    else:
        handler.convert_csv(args.infile, args.outfile)
        
