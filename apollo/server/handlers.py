"""
Defines classes for handling HTTP requests to the solar farm database or other sources. 

These classes are used by the Flask server to respond to user queries. 
"""
import sqlite3
import pandas as pd
import numpy as np
import datetime
from flask import jsonify
import apollo.server.cfg as cfg
import apollo.server.schemas as schemas


import logging
import traceback

logger = logging.getLogger(__name__)

class ServerRequestHandler:
    """Superclass for handling requests made to the Apollo Flask server. 
    
    Each subclass should implement handle_request
    """
    def handle_request(self,request, args):
        try:
            return str(request)
        except Exception as e:
            return 'Bad Request: '+str(e), 400
        
    def log_debug(self,msg, e=None):
        """A simple wrapper method to `logging.info`.
        
        Included to make potential alteration to logging easier. 
        """
        if e is None:
            e = ""
        else:
            e = "\n"+ str(e)
        logger.debug(str(msg)+e)
    
    def log_info(self,msg, e=None):
        """A simple wrapper method to `logging.info`.
        
        Included to make potential alteration to logging easier. 
        """
        if e is None:
            e = ""
        else:
            e = "\n"+ str(e)
        logger.info(str(msg) +  e)
    
    def log_error(self,msg, e=None):
        """A simple wrapper method to `logging.error`.
        
        Included to make potential alteration to logging easier. 
        """
        if e is None:
            e = ""
        else:
            e = "\n"+ str(e)
        logger.error(str(msg) + e)
        traceback.print_exc()
        
class SolarDBRequestHandler(ServerRequestHandler):
    """A class for handling requests to the solar farm Database. 
    
    """
    def handle_request(self, request, dbfile=None, **args):  
        response_dictionary = self.process_args(request, dbfile)
        js = jsonify(response_dictionary)
        self.log_debug("QUERY COMPLETE")
        return js
    
    def process_args(self, query_request, dbfile):
        args = query_request.args
        source =        args.get(cfg.QUERY_SOURCE_KEY,None)
        table =         args.get(cfg.QUERY_SITE_KEY,None)
        attributes =    args.getlist(cfg.QUERY_ATTRIBUTE_KEY)
        statistics =    args.getlist(cfg.QUERY_STATISTIC_KEY)
        groupby =       args.get(cfg.QUERY_GROUPBY_KEY, "")
        start_raw =     args.get(cfg.QUERY_START_KEY,None)
        stop_raw =      args.get(cfg.QUERY_STOP_KEY,None)
    
        start = start_raw
        stop =  stop_raw
        
        # filter out invalid options
        statistics = [st for st in statistics if self.statistic_check(st)]
        attributes = [at for at in attributes if self.attribute_check(source,table,at)]

        try:
            start = datetime.datetime.fromtimestamp(int(start)/ 1e3)
            stop = datetime.datetime.fromtimestamp(int(stop) / 1e3)
        except Exception as e:
            raise Exception(f'Incorrect start or stop time format. Found values are {start} and {stop}.\n' + str(e))
        
        self.log_debug(f"QUERY: source={source}, start={start}, stop={stop}, table={table},attributes={attributes}, groupby={groupby}, statistics={statistics}")
        
        columns =  self.get_sql_columns(statistics, attributes)
        where_clause =  f"WHERE TIMESTAMP >= {int(start.timestamp())} AND TIMESTAMP <= {int(stop.timestamp())}"
        timestamp, groupby_modified = self.get_select_time(groupby)
        
        sql = f"SELECT {timestamp}, {columns} FROM {table} {where_clause} {groupby_modified}"
        
        if groupby in cfg.USE_PROC:
            timestamp = "TIMESTAMP"
        
        df = self.query_db(dbfile,sql)
        response_dict = self.query_format_response(source,table, int(start_raw), int(stop_raw), timestamp,groupby,statistics,df)
        return response_dict

    def get_select_time(self, groupby_key):
        """Using the given key, return a sql GROUP BY statement, together with a
        a name for the column to use as the timestamp. 
        """
        if groupby_key in cfg.GROUP_BY_DICTIONARY:
            groupby = cfg.GROUP_BY_DICTIONARY[groupby_key]["groupby"]
            timestamp = cfg.GROUP_BY_DICTIONARY[groupby_key]["timestamp"]
        else:
            groupby = ""
            timestamp = 'TIMESTAMP'
        return timestamp, groupby
        
    def get_sql_columns(self, statistics=[], attributes=[]):
        '''Constructs arguments for an SQL SELECT statement, possibly using statistics.
        
        Statistics should be a list from ["MIN", "MAX", "AVG", "COUNT"]. 
        Attributes should be a list of attributes in the specified table. 
        
        '''
        # if the stats list is not empty, construct terms for each statistic and attribute. 
        if statistics:
            return ",".join([",".join([f"{stat}("+str(att)+")" for att in attributes]) for stat in statistics])
        # otherwise join the attributes
        else:
            return ",".join(attributes)
    
    def statistic_check(self, exp):
        '''Determines whether string ``exp`` is in the list of approved statistics (``MIN``, ``MAX``, etc.).
        
        Returns:
            ``exp`` if it is a valid expression. Otherwise it raises an exception. 
        '''
        return str(exp).upper() in cfg.STATISTICS
            
    def attribute_check(self, source, table, a):
        '''Determines whether string ``a`` is in the list of columns of ``table``.
        
        Returns:
            ``a`` if it is a valid expression. Otherwise it raises an exception. 
        '''
        try:
            if schemas.get_schema_data(cfg.SCHEMA_DIR,source,table,a) is not None:
                return True
            else:
                return False
        except Exception as ex:
            return False
    
    def query_db(self, dbfile,sql):
        self.log_debug("querying database...")
        self.log_debug(sql)
        df = None
        try:
            conn = sqlite3.connect(dbfile)
            df = pd.read_sql_query(sql, conn)
            self.log_debug("done querying database: " + str(np.size(df)))
        except Exception as e:
            self.log_error(e)
        finally:
            if conn:
                conn.close()
        return df
     
    def strip_statistics(self, name):
        for stat in cfg.STATISTICS:
            if stat in name:
                name = name.replace(stat+"(","").replace(")","")
                return [name, stat]
        return [name, None]
    
    
    
    def query_format_response(self, source, site, start, stop, timestamp_column, groupby, statistics, df):
        self.log_debug("formatting response...")
        columns = []
        rows = []
        title_str = ""
        subtitle_str = ""
        unique_attribute_list = []
        unique_statistics_list = []
        if df is not None:
            if groupby == "monthofyear" or groupby == "dayofyear":
                pass
            else:
                df[timestamp_column] = df[timestamp_column] * 1e3
            # Done because NaN is not part of JSON. None should become null when jsonified. 
            df.replace({pd.np.nan: None}, inplace=True)
            
            rows = df.values.tolist()                
            attribute_list = [ self.strip_statistics(colname) for colname in df]
            for [col_name,statistic] in attribute_list:
                
                if col_name not in unique_attribute_list:
                    unique_attribute_list.append(col_name)
                if statistic and statistic not in unique_statistics_list:
                    unique_statistics_list.append(statistic)
                    
                metadata = schemas.get_schema_data(cfg.SCHEMA_DIR, source, site, col_name)
                statprefix = ""
                if statistic:
                    statprefix = str(statistic) + " "
                coldata = {
                        cfg.OUTPUT_COLUMN_ATTRIBUTE_NAME_KEY:statprefix + str(metadata[cfg.SCHEMA_LABEL_KEY]),
                        cfg.OUTPUT_COLUMN_ATTRIBUTE_UNITS_KEY:metadata[cfg.SCHEMA_UNITS_KEY],
                        cfg.OUTPUT_COLUMN_ATTRIBUTE_LONGNAME_KEY:metadata[cfg.SCHEMA_DESCRIPTION_KEY],
                        cfg.OUTPUT_COLUMN_ATTRIBUTE_DATATYPE_KEY:metadata[cfg.SCHEMA_CHART_DATATYPE_KEY]
                        }
                columns.append(coldata)
            title_str = ",".join(unique_attribute_list)
            subtitle_str = ",".join(unique_statistics_list)
        
        return {
            cfg.OUTPUT_SITE_KEY:  site,
            cfg.OUTPUT_START_TIME_KEY: start,
            cfg.OUTPUT_STOP_TIME_KEY:  stop,
            cfg.OUTPUT_COLUMNS_KEY:  columns,
            cfg.OUTPUT_ROWS_KEY:  rows,
            cfg.OUTPUT_TITLE_KEY:  title_str,
            cfg.OUTPUT_UNITS_KEY:  "",
            cfg.OUTPUT_SUBTITLE_KEY:  subtitle_str}
        
        

############################################################################
        
class SolarDBRequestHandlerPostProcessing(SolarDBRequestHandler):
    
    def get_sql_columns(self, statistics=[], attributes=[]):
        '''Constructs arguments for an SQL SELECT statement, ignoring statistics. 
        
        Attributes should be a list of attributes in the specified table. 
        '''
        return ",".join(attributes)
    
    def strip_statistics(self, name):
        for stat in cfg.STATISTICS:
            stat = stat.upper()
            name = name.upper()
            if stat in name:
                name = name.replace("_"+stat,"")
                return [name, stat]
        return [name, None]

    def query_format_response(self, source, site, start, stop, timestamp_column, groupby, statistics, df):
        self.log_debug("formatting response...")
        columns = []
        rows = []
        title_str = ""
        subtitle_str = ""
        unique_attribute_list = []
        unique_statistics_list = []
        
        if df is not None:
            if groupby == "monthofyear" or groupby == "dayofyear":
                pass
            else:
                df[timestamp_column] = df[timestamp_column] * 1e3
            # Done because NaN is not part of JSON. None should become null when jsonified. 
            statistics_func = [self.get_stat_function(x) for x in statistics]
            if groupby in cfg.GROUP_BY_DICTIONARY:
                self.log_debug("aggregating...")
                groups = df.groupby(cfg.GROUP_BY_DICTIONARY[groupby]["fields"])
                
                timestamps = groups['TIMESTAMP'].min();

                g2 = groups.agg(statistics_func)
                g2.columns = ["_".join(x) for x in g2.columns.ravel()]
                for col_name in g2.columns:
                    if col_name.startswith("TIMESTAMP"):
                        g2.drop([col_name], axis=1,inplace =True)
                g2.insert(0, 'TIMESTAMP',timestamps)
            
                g2.replace({pd.np.nan: None}, inplace=True)
                attribute_list = [ self.strip_statistics(colname) for colname in g2.columns]
                rows = g2.values.tolist()                
            else:
                rows = df.values.tolist()         
                attribute_list = [ self.strip_statistics(colname) for colname in df]
            
            self.log_debug(attribute_list)
            
            for [col_name,statistic] in attribute_list:
                if col_name not in unique_attribute_list:
                    unique_attribute_list.append(col_name)
                if statistic and statistic not in unique_statistics_list:
                    unique_statistics_list.append(statistic)
                    
                metadata = schemas.get_schema_data(cfg.SCHEMA_DIR, source, site, col_name)
                statprefix = ""
                if statistic:
                    statprefix = str(statistic) + " "
                coldata = {
                        cfg.OUTPUT_COLUMN_ATTRIBUTE_NAME_KEY:statprefix + str(metadata[cfg.SCHEMA_LABEL_KEY]),
                        cfg.OUTPUT_COLUMN_ATTRIBUTE_UNITS_KEY:metadata[cfg.SCHEMA_UNITS_KEY],
                        cfg.OUTPUT_COLUMN_ATTRIBUTE_LONGNAME_KEY:metadata[cfg.SCHEMA_DESCRIPTION_KEY],
                        cfg.OUTPUT_COLUMN_ATTRIBUTE_DATATYPE_KEY:metadata[cfg.SCHEMA_CHART_DATATYPE_KEY]
                        }
                columns.append(coldata)
                
            title_str = ",".join(unique_attribute_list)
            subtitle_str = ",".join(unique_statistics_list)
        
            self.log_debug("done formatting.")
        return {
            cfg.OUTPUT_SITE_KEY:  site,
            cfg.OUTPUT_START_TIME_KEY: start,
            cfg.OUTPUT_STOP_TIME_KEY:  stop,
            cfg.OUTPUT_COLUMNS_KEY:  columns,
            cfg.OUTPUT_ROWS_KEY:  rows,
            cfg.OUTPUT_TITLE_KEY:  title_str,
            cfg.OUTPUT_UNITS_KEY:  "",
            cfg.OUTPUT_SUBTITLE_KEY:  subtitle_str}
        

    def get_stat_function(self, label):
        
        def percentile(n):
            def _percentile(x):
                return np.nanpercentile(x, n)
            _percentile.__name__ = 'PER{}'.format(n)
            return _percentile

        def std():
            def _std(x):
                return np.nanstd(x, ddof=1)
            _std.__name__ = 'STD'
            return _std
        def stdp():
            def _stdp(x):
                return np.nanstd(x, ddof=0)
            _stdp.__name__ = 'STDp'
            return _stdp
  
        def var():
            def _var(x):
                return np.nanvar(x, ddof=1)
            _var.__name__ = 'VAR'
            return _var
        def varp():
            def _varp(x):
                return np.nanvar(x, ddof=0)
            _varp.__name__ = 'VARp'
            return _varp
        
        if "MAX" == label:
            return 'max'
        if "MIN" == label:
            return 'min'
        if "MEAN" == label:
            return 'mean'
        if "COUNT" == label:
            return 'count'
        if "SUM" == label:
            return 'sum'
        if "PER5" == label:
            return percentile(5)
        if "PER10" == label:
            return percentile(10)
        if "PER20" == label:
            return percentile(20)
        if "PER25" == label:
            return percentile(25)
        if "PER50" == label:
            return percentile(50)
        if "PER75" == label:
            return percentile(75)
        if "PER90" == label:
            return percentile(90)
        if "PER95" == label:
            return percentile(95)
        if "PER99" == label:
            return percentile(99)
        if "STD" == label:
            return std()
        if "STDP" == label:
            return stdp()
        if "VAR" == label:
            return var()
        if "VARP" == label:
            return varp()
        return 'min'
      
        
        