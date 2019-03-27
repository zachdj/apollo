''' Converts a csv file describing a datasource into a json dictionary.

Each CSV file corresponds to a table in the database.
Each row of the csv describes an attribute of the data source
(essentially a column of a database table).
These are used especially when formatting the data for consumption
by the end user.

This script is intended to generate a single json file describing a data source. 
The json file can be used by the web-based front end.
If needed, it can be manually edited.

The headers of the csv file currently are:
 * index: the column index of the attribute in the corresponding database table.
 * label: the short name identifying of the attribute.
 * description: a longer description of the attribute. 
 * units: the measurement units of the attribute. 
 * sql_datatype: the data type in the database table.
 * chart_datatype: the data type to pass to the client side data renderer.

'''

import json
import os
import pandas as pd
from pathlib import Path

SCHEMA_NAME_KEY =   "name"
SCHEMA_COL_KEY =    "columns"
SCHEMA_LABEL_KEY =  "label"
SCHEMA_DESCRIPTION_KEY = "description"
SCHEMA_UNITS_KEY =  "units"
SCHEMA_CHART_DATATYPE_KEY = "chart_datatype"

schema_dict = {}


def extract_schemas(working_dir, outfile=None):
    """Process csv files in the given directory, returning a dictionary. 
    The keys of the dictionary are the file name of the csv file (without the extension).
    The values are themselves dictionaries of records. 
    """
    schema_list = {}
    for root, dirs, files in os.walk(working_dir):
            for filename in files:
                if filename.endswith('.csv'):
                    name = filename.replace('.csv', '')
                    schema = process_schema_file(name, working_dir+filename)
                    schema_list[name] = schema
    if outfile:
        with open(outfile, 'w') as outf:
            json.dump(schema_list, outf,indent=2)
    return schema_list


def process_schema_file(name, filename):
    ''' Create a dictionary from a csv file, where each row is a dict entry
    '''
    df = pd.read_csv(filename, dtype=str, keep_default_na=False) 
    df1 = df.to_dict(orient='records')
    schema = dict()
    schema[SCHEMA_NAME_KEY] = name
    columns = {}
    for entry in df1:
        key = entry[SCHEMA_LABEL_KEY]
        temp_dict = {}
        for ekey in entry:
            temp_dict[ekey] = entry[ekey]
        columns[key] = temp_dict
    schema[SCHEMA_COL_KEY] = columns
    return schema       


def get_schema_data(schema_dir, source, table, attribute):
    """ Get information about a given column from a db source and table

    The schema needs to be stored in the `schema_dir`.
    
    E.g. the units can be returned.
    """
    global schema_dict 
    if source not in schema_dict:
        sfile = source+".json"
        schema_dict[source] = load_schema_data(Path(schema_dir) / sfile)
    try:
        return schema_dict[source][table][SCHEMA_COL_KEY][attribute]
    except Exception as e:
        print(e)
        return None


def load_schema_data(schema_file):
    with open(schema_file, 'r') as f:
        results = json.load(f)
    return results


def process_csv(source, working_dir, outfile=None):
    schema = {'source': source}
    tables = []
    for root, dirs, files in os.walk(working_dir):
            for filename in files:
                if filename.endswith('.csv'):
                    name = filename.replace('.csv', '')
                    table_schema = process_csv_file(name, working_dir+filename)
                    tables.append(table_schema)
    schema['tables'] = tables
    if outfile:
        with open(outfile, 'w') as outf:
            json.dump(schema, outf, indent=10)

    
def process_csv_file(name, filename_in):
    """ Get table name and column names from a csv file representing a table
    """
    df = pd.read_csv(filename_in, dtype=str, keep_default_na=False)
    return {
        'table': name,
        'columns': df.values.tolist()
    }
    

if __name__ == "__main__":
    # TODO: refactor or remove
    working_dir = "C:/Users/fwmaier/Desktop/python/server/schemas/raw_csv/"
    outfile = working_dir + 'UGASolarArray.json'
    process_csv('UGASolarArray', working_dir, outfile)