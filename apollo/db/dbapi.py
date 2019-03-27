"""
Defines conversion and database access routines for Solar Farm log data.
 
This module defines an API for writing solar farm log data
into an SQLite database.

The input data (logged solar farm data) currently is in two formats.
The earlier batch consists of small, gzipped .log files pushed to UGA servers.
The second batch was downloaded manually from REAPR, an internal website.
 
https://reapr.southernco.com/NonSecure/LoginFrames.aspx?ReturnUrl=%2f 
 
The data is for 8 "modules" (Base, A-E, Irradiance, and 2-axis tracking). 
Arrays A to E are solar arrays at the solar farm. The irradiance module stores 
solar radiation data recorded by multiple sensors (pyronometers). 

Once converted, the data is stored in one of 8 tables (one for each module).
The schemas for the database tables are stored in separate
SQL CREATE TABLE statements.

The routines here assume that the log data is in the proper format for insertion
into the database.
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
    ''' Wrapper method to `logging.info`.

    Included to make potential alteration to logging easier.
    '''
    e = '' if e is None else f'\n{e}'
    logger.debug(str(msg) + e)


def log_info(msg, e=None):
    ''' Wrapper method to `logging.info`.

    Included to make potential alteration to logging easier.
    '''
    e = '' if e is None else f'\n{e}'
    logger.info(str(msg) + e)


def log_error(msg, e=None):
    ''' Wrapper method to `logging.error`.

    Included to make potential alteration to logging easier.
    '''
    e = '' if e is None else f'\n{e}'
    logger.error(str(msg) + e)
    traceback.print_exc()


class DBHandler:
    """ Wrapper for accessing an SQLite database.
    
    Attributes:
        db_file (str):
            The path to the sqlite database file
        conn (Connection):
            A handle to an open database connection (or None)
    """
    
    def __init__(self, db_file):
        """ Initialize a database handler
        
        `self.conn` is initialized to `None`. 
        
        Args:
            db_file (str):
                The path to the sqlite database the handler should use.
        
        """
        self.db_file = db_file
        self.conn = None

    def connect(self):
        """ Open a database connection
        
        Exceptions are suppressed (generating a log entry).
        If an Exception is encountered, None is returned.
        
        Returns:
            ``sqlite3.Connection`` or None:
                A handle to the database connection,
                or None if an exception is caught
        """
        try:
            self.conn = sqlite3.connect(self.db_file)
            return self.conn
        except Exception as e:
            log_error(f'Error connecting to db: {self.db_file}', e)
            return None

    def close(self):
        """ Close the open connection if there is one

        If there is no open connection, ths will have no effect
        """
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        
    def execute(self, sql, commit=False):
        """ Execute a SQL statement
        
        Wraps execution in a try-except block,
        logging any exceptions encountered.
        
        Args:
            sql (str):
                The script to execute
            commit (bool):
                If True, a commit will be performed after executing the query
        
        Returns:
            ``sqlite3.Cursor``: The cursor used to execute the query

        """
        try:
            c = self.conn.cursor()
            c.execute(sql)
            if commit:
                self.conn.commit()
            return c
        except Exception as e:
            log_error(f'Error executing statement: {sql}', e)

    def executescript(self, sql, commit=False):
        """ Execute the given (multi)statement
        
        Wraps execution in a try-except block,
        logging any exceptions encountered.
        
        Args:
            sql (str):
                The script to execute
            commit (bool):
                If True, a commit will be performed after executing the query
        Returns:
            ``sqlite3.Cursor``: The cursor used to execute the script.
        """
        try:
            c = self.conn.cursor()
            c.executescript(sql)
            if commit:
                self.conn.commit()
            return c
        except Exception as e:
            log_error(f'Error executing statement: {sql}', e)

    def drop_table(self, table, commit=True):
        """ Drop a table
        
        If the table does not exist, this will have no effect.
        Errors are suppressed.
                
        Args:
            table (str):
                The name of the table to drop
            commit (bool):
                If True, a commit will be performed after dropping the table
        """
        
        sql = f'DROP TABLE IF EXISTS {table}'
        try:
            c = self.conn.cursor()
            c.execute(sql)
            if commit:
                self.conn.commit()

        except Exception as e:
            log_error(f'Error executing statement: {sql}', e)
    
    def clear_table(self, table, commit=True):
        """ Delete all rows from a table
        
        Errors are suppressed.
        
        Args:
            table (str):
                The name of the table to clear
            commit (bool):
                If True, a commit will be performed after dropping the table
        """
        sql = f'DELETE FROM {table}'
        try:
            c = self.conn.cursor()
            c.execute(sql)
            if commit:
                self.conn.commit()

        except Exception as e:
            log_error(f'Error executing statement: {sql}', e)
    
    def tables(self):
        """ List the names of the tables in the current database.
        
        Returns:
            List[str]: The list of table names in the database
        """
        sql = "select name from sqlite_master where type = 'table'"
        cur = self.execute(sql)
        tables = []
        for t in cur.fetchall():
            tables.append(t[0])
        return tables

    def columns(self, table):
        ''' List column information for the given table
        
        Args:
            table (str):
                The name of the table to examine. 
        Returns:
            List[str]: A list containing information on the table's columns.
        '''
        sql = f'PRAGMA table_info({table});'
        cur = self.execute(sql)
        columns = cur.fetchall()
        return columns

    def column_names(self, table):
        ''' List the column names in the given table.
        
        Args:
            table (str):
                The name of the table to examine. 
        Returns:
            List[str]: a list of table column names.
        '''
        columns = self.columns(table)
        names = []
        for row in columns:
            names.append(row[1])
        return names

    def copy_insert(self, source, target):
        """ Copy one table into another

        In the case of a conflict,
        the records in the target table will be replaced.
        
        Args:
            source_table (str):
                The name of the table to copy from.
            target_table (str):
                The name of the table to copy into.
        """
        statement = f'INSERT OR REPLACE INTO {target} SELECT * FROM {source}'
        self.execute(statement, commit=True)

    def insert_dataframe(self, df, table):
        ''' Insert a pandas dataframe into the specified table.
        
        Uses df.to_sql(). The dataframe must be of the appropriate format.
        If duplicates are found, then preexisting values will be overwritten. 
        
        Args:
            df (pandas.DataFrame):
                The name of the table to copy from. 
            table (str):
                The name of the table to insert into.

        Returns:
            None
        '''
        df.to_sql(table, self.conn, if_exists='replace', index=False)

    def table_pragma(self):
        ''' Get metadata on the database tables as a dictionary.
        
        This is the result of the SQL statement `PRAGMA table_info(TableName)`.
        The results are dictionaries with the following keys: ``index``,
        ``name``, ``type``, ``notnull``, ``default``, ``key``. 

        Returns:
            List[dict]: A list of table metadata
        '''
        
        table_dict = {}
        tables = self.tables()
        for table in tables:
            cur = self.conn.cursor()
            sql = "PRAGMA table_info("+table+");"
            cur.execute(sql)
            columns = cur.fetchall()
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
        """ Read a file into an in-memory string.

        Should only be used for small text files.
        
        Returns:
            str: The string contents of the file.
        """
        with open(filename, 'r') as f:
            return f.read()


class REAPRWrapper:
    """ Converter for csv data from the REAPR site format to the internal
    sqlite database format.
    
    The timestamp of the REAPR file is converted into an integer. Also, 
    YEAR, DAY, HOUR, MINUTE, DAYOFYEAR, as well as three code columns
    (CODE1, CODE2, CODE3) are added after the timestamp to make the data
    in the REAPR files.
    """
    def __init__(self, date_format='%m/%d/%Y %I:%M:%S %p'):
        """ Initialize a REAPRWrapper
        
        The date format indicates how timestamps in log records are to be processed. 
        
        Args:
            date_format (str): 
                The format of input record timestamps. 
            use_headers (bool):
                indicates whether headers should be included in written output (csv) files. 
        
        """
        self.date_format = date_format
        self.use_headers = True
        # add CODE1 CODE2 CODE3 columns when coverting to db format. 
        self.add_codes = True

    def insert_csv(self, db_file, csv_to_insert,
                   table=None, usetemp=True, convert=True):
        ''' Insert a csv file into the appropriate database table.

        If no table is specified then the table is inferred from the columns.
        
        The csv is first read into a dataframe and then the dataframe is inserted.
        If usetemp=True, then the dataframe is first inserted into a temporary
        database table and then copied to the final table
        (the temporary table is then purged).
        
        The temporary table is needed to avoid duplicates throwing errors 
        (df.to_sql() does not appear to allow automatic ignoring of duplicates). 
        
        If convert=True, then the input csv will be transformed first,
        which converts the timestamp to an integer and adds columns for
        year, month, day, hour, minute, and day of year.
 
        Args:
            db_file (str): 
                The filepath of the sqlite database file.
            csv_to_insert (str): 
                The format of input record timestamps. 
            table (str): 
                The name of the table where data will be inserted
            usetemp (bool):
                If True, data will be inserted into a temporary table first.
            convert (bool):
                If True, datetime data will be converted.
        
        '''
        dbh = DBHandler(db_file)
        dbh.connect()
        try:
            df = self.csv_to_df(csv_to_insert, convert=convert)
            if table is None:
                module = self.infer_module(csv_to_insert,df)
            else:
                module = table
            if usetemp:
                dbh.insert_dataframe(df, f'{module}_TEMP')
                dbh.copy_insert(f'{module}_TEMP', module)
                dbh.clear_table(f'{module}_TEMP')
            else:
                dbh.insert_dataframe(df, module)
        except Exception as e:
            log_error(f'Error processing REAPR file {csv_to_insert}.'
                      f'\nPerhaps a data mismatch?', e)
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
        if not convert:
            return pd.read_csv(filename)

        df = pd.read_csv(filename,converters={0:self.date_converter})
        dates = pd.DatetimeIndex(df.iloc[:,0])
        
        # rename columns by removing "-"
        column_names = df.columns.values.tolist()
        for i in range(len(column_names)):
            df.rename(columns={column_names[i]:column_names[i].replace("-","")}, inplace=True)
                
        # add 3 code columns at positions 1,2,3
        # (done for compatibility with ftp data format)
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

        df.insert(1, "TIMESTAMP", df.iloc[:,0].astype(np.int64)/int(1e9))
        df.drop([df.columns[0]], axis='columns', inplace=True)
        
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
        error_count = 0
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
    # TODO: refactor or remove
    wrapper = SolarLogWrapper()
    indir = Path("I:/Solar Radition Project Data April 2018/apollo/server/ingz_raw")
    outfile = Path("I:/Solar Radition Project Data April 2018/apollo/server/out.csv.gz")
    wrapper.concat_logs(indir, outfile)
