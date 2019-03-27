from pathlib import Path

TIMESTAMP =  "TIMESTAMP"
HOME_DIR =   Path("C:/Users/fwmaier/Desktop/python")
HTML_DIR =   HOME_DIR /"server"/"html"
SCHEMA_DIR = HOME_DIR/"server"/"schemas"
DB_FILE =    Path("I:/Solar Radition Project Data April 2018/sqlite/solar_farm_sqlite.db")
DB_DIR =    Path("I:/Solar Radition Project Data April 2018/sqlite/")

###################
STATISTICS = set(["MIN", "MAX", "AVG", "MEAN", "COUNT", "SUM", "PER5","PER10","PER20", "PER25","PER50","PER75","PER90","PER95","PER99", "VAR", "STD", "VARP", "STDP"])


###################
OUTPUT_SITE_KEY =       "site"
OUTPUT_TITLE_KEY =      "title"
OUTPUT_SUBTITLE_KEY =   "subtitle"
OUTPUT_START_TIME_KEY = "start"
OUTPUT_STOP_TIME_KEY =  "stop"
OUTPUT_UNITS_KEY =      "units"
OUTPUT_ROWS_KEY =       "rows"
OUTPUT_COLUMNS_KEY =    "columns"
###################
OUTPUT_COLUMN_ATTRIBUTE_NAME_KEY =      "label"
OUTPUT_COLUMN_ATTRIBUTE_LONGNAME_KEY =  "longname"
OUTPUT_COLUMN_ATTRIBUTE_UNITS_KEY =     "units"
OUTPUT_COLUMN_ATTRIBUTE_DATATYPE_KEY =  "type"
OUTPUT_COLUMN_ATTRIBUTE_DESCRIPTION_KEY = "description"
###################
SCHEMA_NAME_KEY =   "name"
SCHEMA_COL_KEY =    "columns"
SCHEMA_LABEL_KEY =  "label";
SCHEMA_DESCRIPTION_KEY = "description";
SCHEMA_UNITS_KEY =  "units";
SCHEMA_CHART_DATATYPE_KEY = "chart_datatype";

QUERY_SOURCE_KEY =      "source"
QUERY_SITE_KEY =        "site"
QUERY_ATTRIBUTE_KEY =   "attribute"
QUERY_STATISTIC_KEY =   "statistic"
QUERY_GROUPBY_KEY =     "groupby"
QUERY_START_KEY =       "start"
QUERY_STOP_KEY =        "stop"
###################
"""
GROUP_BY_DICTIONARY ={
		"yearmonthdayhourmin": "GROUP BY YEAR, MONTH, DAY, HOUR, strftime('%M', datetime(TIMESTAMP, 'unixepoch'))",
		"yearmonthdayhour": "GROUP BY YEAR, MONTH, DAY, HOUR",
		"yearmonthday": "GROUP BY YEAR, MONTH, DAY",
		"yearmonth": "GROUP BY YEAR, MONTH ORDER BY YEAR, MONTH",
		"dayhourofyear": "GROUP BY DAYOFYEAR, HOUR",
		"monthofyear": "GROUP BY MONTH ORDER BY MONTH",
       "dayofyear": "GROUP BY DAYOFYEAR",
       "proc_yearmonthdayhourmin": "",
       "proc_yearmonthdayhour": "",
       "proc_yearmonthday": "",
       "proc_yearmonth": ""       
		}

TIME_SELECT_DICTIONARY = {
		"yearmonthdayhourmin": "MIN(TIMESTAMP)",
		"yearmonthdayhour": "MIN(TIMESTAMP)",
		"yearmonthday": "MIN(TIMESTAMP)",
		"yearmonth": "MIN(TIMESTAMP)",
		"dayhourofyear": "MIN(TIMESTAMP)",
		"monthofyear": "MONTH",
       "dayofyear": "DAYOFYEAR",
       "proc_yearmonthdayhourmin": "TIMESTAMP, YEAR, MONTH, DAY, HOUR, MINUTE",
       "proc_yearmonthdayhour": "TIMESTAMP, YEAR, MONTH, DAY, HOUR",
       "proc_yearmonthday": "TIMESTAMP, YEAR, MONTH, DAY",
       "proc_yearmonth": "TIMESTAMP, YEAR, MONTH"       
		}
"""
USE_PROC = ["proc_yearmonthdayhourmin","proc_yearmonthdayhour","proc_yearmonthday","proc_yearmonth"]

GROUP_BY_DICTIONARY = {
       "yearmonthdayhourmin": {
               "groupby":"GROUP BY YEAR, MONTH, DAY, HOUR, strftime('%M', datetime(TIMESTAMP, 'unixepoch'))",
               "fields":[],
               "timestamp": "MIN(TIMESTAMP)"
               },
		"yearmonthdayhour": {
                "groupby": "GROUP BY YEAR, MONTH, DAY, HOUR",
               "fields":[],
                "timestamp": "MIN(TIMESTAMP)"
                       },
		"yearmonthday": {
                "groupby":"GROUP BY YEAR, MONTH, DAY",
               "fields":[],
                "timestamp": "MIN(TIMESTAMP)"},
		"yearmonth": {
                "groupby":"GROUP BY YEAR, MONTH ORDER BY YEAR, MONTH",
               "fields":[],
                "timestamp": "MIN(TIMESTAMP)"},
		"dayhourofyear": {
                "groupby": "GROUP BY DAYOFYEAR, HOUR",
               "fields":[],
                "timestamp": "MIN(TIMESTAMP)"},
		"monthofyear": {
                "groupby": "GROUP BY MONTH ORDER BY MONTH",
               "fields":[],
                "timestamp": "MONTH"},
       "dayofyear": {
               "groupby":"GROUP BY DAYOFYEAR",
               "fields":[],
               "timestamp": "DAYOFYEAR"},
       "proc_yearmonthdayhourmin": {
               "groupby":"",
               "fields":['YEAR','MONTH', 'DAY', 'HOUR','MINUTE'],
               "timestamp": "TIMESTAMP, YEAR, MONTH, DAY, HOUR, MINUTE"},
       "proc_yearmonthdayhour": {
               "groupby":"",
               "fields":['YEAR','MONTH', 'DAY', 'HOUR'],
               "timestamp": "TIMESTAMP, YEAR, MONTH, DAY, HOUR"},
       "proc_yearmonthday": {
               "groupby":"",
               "fields":['YEAR','MONTH', 'DAY'],
               "timestamp": "TIMESTAMP, YEAR, MONTH, DAY"},
       "proc_yearmonth": {
               "groupby":"",
               "fields":['YEAR','MONTH'],
               "timestamp": "TIMESTAMP, YEAR, MONTH" 
               }
        }

