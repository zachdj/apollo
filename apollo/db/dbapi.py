"""
Defines conversion and database access routines for Solar Farm log data.
 
This module defines an API for writing solar farm log data into an SQLite database. 

The input data (logged solar farm data) currently is in two formats. The earlier batch consists of 
small, gzipped .log files pushed to UGA servers. The second batch was downloaded manually from REAPR, an internal website.
 
https://reapr.southernco.com/NonSecure/LoginFrames.aspx?ReturnUrl=%2f 
 
The data is for 8 "modules" (Base, A-E, Irradiance, and 2-axis tracking). 
Arrays A to E are solar arrays at the solar farm. The irradiance module stores 
solar radiation data recorded by multiple sensors (pyronometers). 

Once converted, the data is stored in one of 8 tables (one for each module).
The schemas for the database tables are stored in separate SQL CREATE TABLE statements. 

The routines here assume that the log data is in the proper format for insertion into the database.
""" 

import sqlite3
import numpy as np
import pandas as pd
import os
import datetime
import gzip
import logging
from pathlib import Path
import traceback

logger = logging.getLogger(__name__)


def log_debug(msg, e=None):
    """A simple wrapper method to `logging.debug`.
    Included to make potential alteration to logging easier. 
    """
    if e is None:
        e = ""
    else:
        e = "\n"+ str(e)
    logger.debug(str(msg)+e)

def log_info(msg, e=None):
    """A simple wrapper method to `logging.info`.
    
    Included to make potential alteration to logging easier. 
    """
    if e is None:
        e = ""
    else:
        e = "\n"+ str(e)
    logger.info(str(msg) +  e)

def log_error(msg, e=None):
    """A simple wrapper method to `logging.error`.
    
    Included to make potential alteration to logging easier. 
    """
    if e is None:
        e = ""
    else:
        e = "\n"+ str(e)
    logger.error(str(msg) + e)
    traceback.print_exc()

class DBHandler:
    """A wrapper for accessing an SQLite database. 
    
    Attributes:
    
    db_file : str
        the path to the sqlite database file
    conn : 
        a handle to an open database connection (or None)
    """
    
    def __init__(self,dbfile):
        """Create an instance of the database handler, storing the database file.
        
        `self.conn` is initialized to `None`. 
        
        Args:
            dbfile (str): 
                the path to the sqlite database the handler should use. 
        
        """
        self.db_file = dbfile
        self.conn = None

    def connect(self):
        """Connect to the database, storing (and returning) a handle to the connection. 
        
        Exceptions are suppressed (generating a log entry). If an Exception is encountered, None is returned. 
        
        Returns:
            ``sqlite3.Connection``: a handle to the database connection. 
        """
        try:
            self.conn = sqlite3.connect(self.db_file)
            return self.conn
        except Exception as e:
            log_error(f'Error connecting to db: {self.db_file}' , e)
            return None

    def close(self):
        """Close an open connection if there is one; otherwise do nothing. 
        
        Resets ``self.conn`` to ``None``.
        """
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        
    def execute(self, sql, commit=False):
        """Creates a cursor and executes the given statement, returning the cursor.
        
        Wraps execution in a try-except block, logging any exceptions encountered. 
        
        Args:
            sql (str):
                The script to execute
            commit (bool):
                Determines whether or not a commit is performed after execution. 
        
        Returns:
            ``sqlite3.Cursor``: a handle to the cursor. 

        """
        try:
            c = self.conn.cursor()
            c.execute(sql)
            if commit:
                self.conn.commit()
            return c; 
        except Exception as e:
            log_error(f'Error executing statement: {sql}', e)

    def executescript(self,sql, commit=False):
        """Creates a cursor and executes the given (multi)statement, returning the cursor.
        
        Wraps execution in a try-except block, logging any exceptions encountered. 
        
        Args:
            sql (str):
                The script to execute
            commit (bool):
                Determines whether or not a commit is performed after execution. 
        Returns:
            ``sqlite3.Cursor``: a handle to the cursor. 
        """
        try:
            c = self.conn.cursor()
            c.executescript(sql)
            if commit:
                self.conn.commit()
            return c; 
        except Exception as e:
            log_error(f'Error executing statement: {sql}', e)

    def drop_table(self, table, commit=True):
        """Drops a table, whether or not it exists. 
        
        A try-except block is used to suppress
        an error if the table does not already exist. 
                
        Args:
            table (str):
                The name of the table to drop
            commit (bool):
                Determines whether or not a commit is performed after execution. 
        """
        
        sql = "DROP TABLE IF EXISTS " + table

        try:
            c = self.conn.cursor()
            c.execute(sql);
            if commit:
                self.conn.commit()

        except Exception as e:
            log_error(f'Error executing statement: {sql}', e)

    
    def clear_table(self, table, commit=True):
        """deletes all rows from a table. 
        
        A try-except block is used to suppress errors. 
        
        Args:
            table (str):
            commit (bool):
                Determines whether or not a commit is performed after execution. 
        """
        
        sql = "DELETE FROM " + table
        try:
            c = self.conn.cursor()
            c.execute(sql);
            if commit:
                self.conn.commit()

        except Exception as e:
            log_error(f'Error executing statement: {sql}', e)

    
    def tables(self):
        """Returns a list of table names in the current database.
        
        Returns:
            ``list``: a list of strings (table names)
        """
        sql = "select name from sqlite_master where type = 'table'";
        cur = self.execute(sql)
        tables = [];
        for t in cur.fetchall():
            tables.append(t[0])
        return tables

    def columns(self, table):
        """Returns a list of entries containing table column information 
        
        Args:
            table (str):
                The name of the table to examine. 
        Returns:
            ``list``: a list containing information on the table columns. 
        """
        sql = "PRAGMA table_info("+table+");"
        cur = self.execute(sql)
        columns = cur.fetchall();
        return columns

    def column_names(self, table):
        """Returns a list names of columns in the given table table.
        
        Args:
            table (str):
                The name of the table to examine. 
        Returns:
            ``list``: a list of table column names. 
        """
        columns = self.columns(table)
        names = [];
        for row in columns:
            names.append(row[1])
        return names

    def copy_insert(self, source_table,target_table):
        """Copies one table into another, replacing records in the target if there is a conflict.
        
        Args:
            source_table (str):
                The name of the table to copy from. 
            target_table (str):
                The name of the table to copy into.
        """
        statement = "INSERT OR REPLACE INTO " + target_table + " SELECT * FROM " + source_table
        self.execute(statement, commit=True)

    def insert_dataframe(self, df,table):
        """Inserts a pandas dataframe into the specified table. 
        
        Uses df.to_sql(). The databframe must be of the appropriate format. 
        If duplicates are found, then preexisting values will be overwritten. 
        
        Args:
            df (pandas.DataFrame):
                The name of the table to copy from. 
            table (str):
                The name of the table to insert into.
       
        """
        df.to_sql(table, self.conn, if_exists='replace', index=False)

    def table_pragma(self):
        """Returns meta data on the database tables as a dictionary.
        
        This is the result of the SQL statement `PRAGMA table_info(TableName)`.
        The results are dictionaries with the following keys: ``index``,
        ``name``, ``type``, ``notnull``, ``default``, ``key``. 

        Returns:
            ``list``: a list of dictionaries. 
        """
        
        table_dict = {}
        tables = self.tables()
        for table in tables:
            cur = self.conn.cursor()
            sql = "PRAGMA table_info("+table+");"
            cur.execute(sql)
            columns = cur.fetchall();
            col_list = []
            for row in columns:
                col_list.append({
                    "index": row[0],
                    "name": row[1],
                    "type": row[2],
                    "notnull": row[3],
                    "default":row[4],
                    "key": row[5]})
                table_dict[table] = col_list

        return table_dict
    
    def file_to_string(self, filename):
        """Reads in a file as a string. Should only be used for small text files.
        
        
        Returns:
            ``str``: the string contents of the file. 
        """
        with open(filename, 'r') as f:
            return f.read()
    
    


class REAPRWrapper:
    """A class for converting data, stored in csv files, from the REAPR site format
    to the internal sqlite database format. 
    
    The timestamp of the REAPR file is converted into an integer. Also, 
    YEAR, DAY, HOUR, MINUTE, DAYOFYEAR, as well as three code columns (CODE1, CODE2, CODE3) 
    are added after the timestamp to make the data in the REAPR files. 

    Attributes:
    
    date_format : str
        the format (e.g., '%m/%d/%Y %I:%M:%S %p') to use when parsing dates in input files. 
    use_headers (boolean): 
        indicates whether headers should be written to output (csv) files. 
    """
    
    def __init__(self,date_format='%m/%d/%Y %I:%M:%S %p'):
        """Create an instance of the wrapper, storing the provided date format (if any). 
        
        The date format indicates how timestamps in log records are to be processed. 
        
        Args:
            date_format (str): 
                The format of input record timestamps. 
            use_headers (boolean): 
                indicates whether headers should be included in written output (csv) files. 
        
        """
        self.date_format = date_format
        self.use_headers = True
        
        # add CODE1 CODE2 CODE3 columns when coverting to db format. 
        self.add_codes = True 

        # ### Process REAPR Files
    def insert_csv(self, db_file,csv_to_insert,table=None, usetemp=True, convert=True):
        """insert a csv file into the appropriate database table. If no table is specified, 
        then the table is inferred from the column names. 
        
        The csv is first read into a dataframe and then the dataframe is inserted. If usetemp=True, 
        then the dataframe is first inserted into a temporary database table and then 
        copied to the final table (the temporary table is then purged). 
        
        The temporary table is needed to avoid duplicates throwing errors 
        (df.to_sql() does not appear to allow automatic ignoring of duplicates). 
        
        If convert=True, then the input csv will be transformed first, which converts the timestamp to 
        an integer and adds columns for year, month, day, hour, minute, and day of year. 
 
        Args:
            db_file (str): 
                The format of input record timestamps. 
            csv_to_insert (str): 
                The format of input record timestamps. 
            table (str): 
                The format of input record timestamps. 
            usetemp (boolean): 
                indicates whether data should be inserted into a temporary table first. 
            convert (boolean): 
                indicates whether the data should be converted (timestamps used to generated year, month, etc., columns). 
        
        """
        dbh = DBHandler(db_file)
        dbh.connect()
        try:
            df = self.csv_to_df(csv_to_insert, convert=convert)
            if table == None:
                module = self.infer_module(csv_to_insert,df)
            else:
                module = table
            if usetemp:
                dbh.insert_dataframe(df, module+"_TEMP")
                dbh.copy_insert(module+"_TEMP",module)
                dbh.clear_table(module+"_TEMP")
            else:
                dbh.insert_dataframe(df, module)
        except Exception as e:
            log_error(f"Error processing REAPR file {csv_to_insert}.\nPerhaps a data mismatch?" ,e)
        dbh.close()
    
    def date_converter(self, timestamp):
        """Constructs a ``datetime.datetime`` object from the given input. 
        Uses the stored datetime format to parse input timestamps. 
        
        Args:
            timestamp (str): 
                The timestamp to convert. 
        Returns:
            ``datetime.datetime``: a datetime representation of the timestamp. 
        """
        return datetime.datetime.strptime(timestamp, self.date_format)
    
    def convert_csv(self, infile, outfile, compression='gzip'):
        """Use Pandas to read a csv file, parsing the date and making the format consistent with the internal database format. 
        Outputs the results to another file. 
        
        The timestamp of the REAPR file is converted into an integer. Also, 
        YEAR, DAY, HOUR, MINUTE, DAYOFYEAR, as well as three code columns (CODE1, CODE2, CODE3) 
        are added after the timestamp to make the data in the REAPR files. 
        
        Note that, as an intermediary step, the whole input file is read into memory as a dataframe. 
    
        Args:
            infile (str):
                The name of the file to process.
            outfile (str):
                The name of the output file.
            compression (str):
                indicates whether the output should be compressed. Default value is 'gzip'
       """
        log_debug(f"IN: {infile}. OUT: {outfile}")
        df = self.csv_to_df(infile)
        df.to_csv(outfile, index = None, header=self.use_headers, compression=compression)
        log_debug("Done")
        

    def convert_dir(self, file_dir, out_dir, compression='gzip'):
        """Use Pandas to convert all csv/gz files in specified directory to the internal database format, writing new files to an output directory. 
        
         Args:
            file_dir (str):
                The name of the directory containing the files to process.
            outfile (str):
                The name of the output directory. 
            compression (str):
                indicates whether the output should be compressed. Default value is 'gzip'
       """
        log_debug(f"converting {file_dir}")        
        for root, dirs, files in os.walk(file_dir):
            for filename in files:
                infile = Path(root)/filename
                if compression == 'gzip' and not filename.endswith('.gz'):
                    gz = filename + '.gz'
                    outfile = Path(out_dir)/gz
                else:
                    outfile = Path(out_dir)/filename
                try:
                    self.convert_csv(infile,outfile,compression=compression)
                except Exception as e:
                    log_error(f"Error processing REAPR file {filename}", e)
    def csv_to_df(self, filename, convert=True):
        """Use Pandas to read a csv file, possibly parsing the date and making the format consistent with the internal database format. 
        
        In the format conversion, the following is performed: 
            
        1) The first column is renamed TIMESTAMP and the values are converted to Unix integer timestamps. It is assumed the first column contains the timestamps. 
        2) Additional fields YEAR,MONTH,DAY,HOUR, MINUTE, DAYOFYEAR are added to speed SELECT queries based on day, month, etc. 
        3) Dummy columns CODE1,CODE2,CODE3 are added after the timestamp to conform with the log data provided by the ftp client. 
        
        Note that the whole file is read into memory. 
    
        Args:
            filename (str):
                The name of the file to process.
            convert (boolean):
                inddicates whether the data should be converted.
        
        Returns:
            ``Pandas.DataFrame``: A dataframe created from the csv data. 
       """
        # load data from csv into pandas dataframe
        #df = pd.read_csv(filename,converters={'MetricDate':pd.to_datetime}))
        if not convert:
            return pd.read_csv(filename)

        #df = pd.read_csv(filename,converters={'MetricDate':self.date_converter})
        #dates = pd.DatetimeIndex(df['MetricDate'])

        df = pd.read_csv(filename,converters={0:self.date_converter})
        dates = pd.DatetimeIndex(df.iloc[:,0])
        
        # rename columns by removing "-"
        column_names = df.columns.values.tolist()
        for i in range(len(column_names)):
            df.rename(columns={column_names[i]:column_names[i].replace("-","")}, inplace=True)
                
        # add 3 code columns at positions 1,2,3 (done for compatibility with ftp data format)
        if self.add_codes:
            df.insert(1, "CODE3", 0) 
            df.insert(1, "CODE2", 0) 
            df.insert(1, "CODE1", 0) 
    
        # add more date-time fields, for faster query execution
        df.insert(1, "DAYOFYEAR",   dates.dayofyear)
        df.insert(1, "MINUTE",      dates.minute)
        df.insert(1, "HOUR",        dates.hour)
        df.insert(1, "DAY",         dates.day) 
        df.insert(1, "MONTH",       dates.month) 
        df.insert(1, "YEAR",        dates.year) 
        
        # add a unix representation of the timestamp (done because SQLite doesn't have a date datatype).
        #df.insert(1, "TIMESTAMP", df['MetricDate'].astype(np.int64)/int(1e9))
        #df.drop(columns=['MetricDate'],inplace=True)

        df.insert(1, "TIMESTAMP", df.iloc[:,0].astype(np.int64)/int(1e9))
        #df.drop(columns=[0],inplace=True)
        df.drop([df.columns[0]] ,  axis='columns', inplace=True)
        
        return df
    
    def infer_module(self, in_filename, df):
        """
        Examine the file name or dataframe column names to infer the solar farm module consistent
        with the dataframe.
        
        When dealing with csv files from the REAPR site, column names are used. For gzipped log files (without headers), 
        the file name itself is used in an attempt to infer the table to use (e.g., 'mb-007' maps to IRRADIANCE). 
        
        Args:
            in_filename (str):
                The name of the input file. 
            df (``Pandas.DataFrame``):
                The dataframe to process
        
        Returns:
            str: One of "BASE", "A","B","C","D","E","IRRADIANCE","TRACKING", or ``None``.
      
        """
        if 'UGAMET01WINDSPD' in df.columns:
            return "BASE"
        if 'UGAAINV01INVSTATUS' in df.columns:
            return "A"
        if 'UGABINV01INVSTATUS' in df.columns:
            return "B"
        if 'UGACINV01INVSTATUS' in df.columns:
            return "C"
        if 'UGADINV01INVSTATUS' in df.columns:
            return "D"
        if 'UGAEINV01INVSTATUS' in df.columns:
            return "E"
        if 'UGAAPOA1IRR' in df.columns:
            return "IRRADIANCE"
        if 'UGAATRACKER01AZMPOSDEG' in df.columns:
            return "TRACKING"
        log_info("NO MODULE FOUND!")
        return None
    
   
class SolarLogWrapper(REAPRWrapper):
    """A class for converting gzipped log files to the internal database format. 

    For roughly 1.5 years, log files from the solar farm were pushed to UGA via an FTP client. 
    The files were very small (covering only a few dozen observations), and 
    compressed in `gz` format. Given the interval covered by each file and the 
    period over which they were generated, we now have many thousands of them, 
    to the point that working with them is cumbersome. 
 
    This class contains routines that can be used to unpack the log files and concatenate them into a single large log file. 
    
    It is assumed that all files for a given module (mb-001 to mb-008) are in a separate subdirectory (bearing the name 

    Attributes:
    
    date_format : str
        the format (e.g., '%m/%d/%Y %I:%M:%S %p') to use when parsing dates in input files. 
    use_headers (boolean): 
        indicates whether headers should be written to output (csv) files. By default, headers are not written. 
    """


    def __init__(self,date_format="'%Y-%m-%d %H:%M:%S'"):
        """Create an instance of the wrapper, storing the provided date format (if any). 
        
        The date format indicates how timestamps in log records are to be processed. 
        
        Args:
            date_format (str): 
                The format of input record timestamps. 
        
        """
        self.date_format = date_format
        self.use_headers = False

    def csv_to_df(self,filename, convert=True):
        """Read in a csv or gzipped csv file, possible converting it to a Pandas dataframe. 
        
        Timestamps (the first column) are converted to to Unix integer timestamps.
        Additional fields YEAR,MONTH,DAY,HOUR, MINUTE, DAYOFYEAR are added to speed SELECT queries based on day, month, etc. 
        
        Args:
            filename (str):
                The file to process
            convert (boolean):
                inddicates whether the data should be converted.
        Returns:
            ``Pandas.DataFrame``: The generated dataframe. 
        """
        df = None
        try:
            if str(filename).endswith(".gz") or str(filename).endswith(".csv") :
                if not convert:
                    df = pd.read_csv(filename)
                    return df
                df = pd.read_csv(filename, header=None,converters={0:self.date_converter})
                dates = pd.DatetimeIndex(df[0])
                # add more date-time fields, for faster query execution
                df.insert(1, "DAYOFYEAR", dates.dayofyear)
                df.insert(1, "MINUTE", dates.minute)
                df.insert(1, "HOUR", dates.hour)
                df.insert(1, "DAY", dates.day) 
                df.insert(1, "MONTH", dates.month) 
                df.insert(1, "YEAR", dates.year) 
                # add a unix representation of the timestamp (done because SQLite doesn't have a date datatype).
                df.insert(1, "TIMESTAMP", df[0].astype(np.int64)/int(1e9))
                df.drop(columns=[0],inplace=True)
        except Exception as e:
            log_error(f"Error converting {filename} to dataframe.",e)
        return df


    def infer_module(self, in_filename, df):
        """
        Examine the dataframe column names to infer the solar farm module consistent
        with the dataframe.
        
        This looks at the column names. If it finds a name that matches an identifying
        column of one of the solar farm modules, it returns that module's name. 
        
        Args:
            df (``Pandas.DataFrame``):
                The dataframe to process
        
        Returns:
            str: One of "BASE", "A","B","C","D","E","IRRADIANCE","TRACKING", or ``None``.
      
        """
        
        in_filename= str(in_filename)
        
        if 'mb-001' in in_filename:
            return "BASE"
        if 'mb-002' in in_filename:
            return "A"
        if 'mb-003' in in_filename:
            return "B"
        if 'mb-004' in in_filename:
            return "C"
        if 'mb-005' in in_filename:
            return "D"
        if 'mb-006' in in_filename:
            return "E"
        if 'mb-007' in in_filename:
            return "IRRADIANCE"
        if 'mb-008' in in_filename:
            return "TRACKING"
        log_info("NO MODULE FOUND!")
        return None

    def concat_logs(self,in_dir, out_file):
        """Concats multiple csv files (in gz format) into one file (again gz).
        
        Each file in the specified directory is read and 
        concatenated into a single file bearing the specified output file name. 
        
        All files in the input directory are used (no filtering of invalid formats is performed).
        It is assumed that the csv files do not contain header information.
        
        Args:
            in_dir (str):
                The directory containing the gzipped files to concatenate. 
            out_file (str):
                The output file to generate.
        """
        error_count = 0; 
        with gzip.open(out_file, 'wb') as outfile:
            for root, dirs, files in os.walk(in_dir):
                log_info(f"\nProcessing: {in_dir}")
                counter = 0
                for filename in files:
                    counter = counter + 1
                    try:
                        f = gzip.open(in_dir/filename, 'rb')
                        outfile.write(f.read())
                        f.close()
                    except Exception as err:
                        log_error("Error with:", err)
                        error_count = error_count + 1
                    if counter % 1000 == 0:
                        log_info(f"Files processed: {counter}, error={error_count}")
            log_info(f"Files processed: {counter}, error={error_count}")            


if __name__ == "__main__":
    wrapper = SolarLogWrapper()
    indir = Path("I:/Solar Radition Project Data April 2018/apollo/server/ingz_raw")
    outfile = Path("I:/Solar Radition Project Data April 2018/apollo/server/out.csv.gz")
    wrapper.concat_logs(indir, outfile)
        