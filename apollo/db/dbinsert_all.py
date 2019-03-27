

import apollo.db.dbapi as dbapi
import apollo.db.dbinit as dbinit
from pathlib import Path
import os

import logging
logger = logging.getLogger(__name__)

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


if __name__ == "__main__":
    
    gz_files = Path("I:/Solar Radition Project Data April 2018/dist/bigdata/in/gz")
    reapr_files = Path("I:/Solar Radition Project Data April 2018/dist/bigdata/in/REAPR")
    
    outfile = "I:/Solar Radition Project Data April 2018/dist/bigdata/out/solar_farm_sqlite.db"
    
    tables = ["BASE", "A", "B","C","D","E","IRRADIANCE","TRACKING"]
    logging.basicConfig(format='[{asctime}] {levelname}: {message}', style='{', level="INFO")

    #create db
    """
    dbinit.init_solar_db(outfile)
    
    handler = dbapi.SolarLogWrapper()
    for table in tables: 
        table_file = table + ".csv.gz"
        print(table_file)
        infile = gz_files/table_file
        
        handler.insert_csv(outfile, infile, table=table,usetemp=True, convert=True)
    """
    
    handler = dbapi.REAPRWrapper()
    for table in tables: 
        print(reapr_files/table)
        process_reapr(outfile, reapr_files/table, table=table,  convert=True)
    
    
    