"""
Generates html files from JSON forecast files. 

The forecasts are the JSON output of the machine learning algorithms. They contain 
tabular data together with meta data describing the names and units of features, as well as the 
reference time  and temporal range of the forecast. 

The JSON is simply combined with a template and saved as an HTML file which can then be served as a static file to end users. 
An index file, containing links to the generated HTML forecast files, is also generated.

The JSON file contents simple replace the string "<MODEL>" in the template. 
Similarly, the index data replaces "<MODELINDEX>" in the index template. 

EXAMPLE: 
>python -m apollo.server.model_html -i forecast_json -o html\solar\forecasts --index schemas\html\index.html --template schemas\html\forecast.html
[2019-03-21 18:15:22,503] INFO:  * in:forecast_json
[2019-03-21 18:15:22,503] INFO:  * out:html\solar\forecasts
[2019-03-21 18:15:22,505] INFO:  * index:schemas\html\index.html
[2019-03-21 18:15:22,507] INFO:  * template:schemas\html\forecast.html

"""
import argparse
import os
import json
import datetime
import logging
from pathlib import Path

def generate_model_index(in_dir, out_dir, index_file, template_file):
    forecasts = []
    for root, dirs, files in os.walk(in_dir):
        for filename in files:
            if filename.endswith(".json"):
                logging.info("processing " +str(filename))
                filestr = file_to_string(Path(root)/ filename)
                data = json.loads(filestr)
                summary = extract_summary(data)
                forecasts.append(summary)
                htmlfile = get_file_name(data)+".html"
                summary['href'] = htmlfile
                template_str = file_to_string(template_file)
                template_str = template_str.replace("<MODEL>", "model_data=\n"+filestr)
                string_to_file( out_dir/htmlfile,template_str)
    index = json.dumps(forecasts)
    index_str = file_to_string(index_file)
    index_str = index_str.replace("<MODELINDEX>", "forecast_index=\n"+index)
    string_to_file(out_dir/"index.html",index_str)
        
def get_file_name(data):
    undefined = "UNDEFINED"
    source = ""
    created = undefined
    keys = data.keys()
    if 'source' in keys:
        source = data['source']
    if 'reftime' in keys:
        created = data['created']
        created = datetime.datetime.fromtimestamp(created / 1e3)
        created = str(created).replace(":","_")
    elif 'created' in keys:
        created = data['created']
        created = datetime.datetime.fromtimestamp(created / 1e3)
        created = str(created).replace(":","_")
    return source+"_"+created
    
def extract_summary(data):
    results= {}
    for k in data.keys():
        if k != "rows":
            results[k] = data[k];
    return results

def file_to_string(filename):
  with open(filename, 'r') as f:
            return f.read()
        
def string_to_file(filename, data):
  with open(filename, 'w') as f:
        f.write(data)



def config_from_args():
    parser = argparse.ArgumentParser(description="""Utility function generating HTML files from JSON forecast files (the output of machine learning models). for converting logged data from the solar farm.
                                     
EXAMPLE: 
>python -m apollo.server.model_html -i forecast_json -o html\solar\forecasts --index schemas\html\index.html --template schemas\html\forecast.html
""")
    parser.add_argument('-i', '--in', metavar='in', type=str, nargs=1,dest='indir',default=None,required=True,help='the directory containing JSON forecasts for process.')
    parser.add_argument('-o', '--out', metavar='out', type=str, nargs=1,dest='outdir',default=None,required=True,help='the directory to store the generated HTML files.')
    parser.add_argument('-n', '--index', metavar='index', type=str, nargs=1,dest='index',default="index.html",required=True,help='the name of the index file ("index.html" by default).')
    parser.add_argument('-t', '--template', metavar='index', type=str, nargs=1,dest='template',default="template.html",required=True,help='the name of the template file to insert the JSON into ("template.html" by default).')
    parser.add_argument('--log', type=str, default='INFO', help='Sets the log level. One of INFO, DEBUG, ERROR, etc. Default is INFO')
    
    args = parser.parse_args()

    logging.basicConfig(format='[{asctime}] {levelname}: {message}', style='{', level=args.log)
    

    if isinstance(args.indir,list):
        args.indir = args.indir[0]
    if isinstance(args.outdir,list):
        args.outdir = args.outdir[0]
    if isinstance(args.index,list):
        args.index = args.index[0]
    if isinstance(args.template,list):
        args.template = args.template[0]
        
    logging.info(" * in:" +str(args.indir))
    logging.info(" * out:"+str(args.outdir))
    logging.info(" * index:"+str(args.index))
    logging.info(" * template:"+str(args.template))
    return args

if __name__ == "__main__":
    args = config_from_args()
    generate_model_index(Path(args.indir),Path(args.outdir), Path(args.index), Path(args.template))
    
