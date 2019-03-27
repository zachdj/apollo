''' Generates html files from JSON forecast files.

The forecasts are the JSON output of the machine learning algorithms.
They contain tabular data together with meta data describing the names
and units of features, as well as the reference time
and temporal range of the forecast.

The JSON is simply combined with a template and saved as an HTML file which can
then be served as a static file to end users.
An index file, containing links to the generated HTML forecast files,
is also generated.

The JSON file contents simple replace the string '<MODEL>' in the template. 
Similarly, the index data replaces '<MODELINDEX>' in the index template. 

EXAMPLE: 
python -m apollo.bin.predictions_to_html
    --in /path/to/forecasts --out /path/to/generated/html html\solar\forecasts
    --index schemas\html\index.html --template schemas\html\forecast.html

'''
import argparse
import datetime
import json
import logging
import os
from pathlib import Path

import apollo.storage as storage


def generate_model_index(in_dir, out_dir, index_file, template_file):
    # make sure output directory exists
    out_dir = Path(out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    forecasts = []
    for root, dirs, files in os.walk(in_dir):
        for filename in files:
            if filename.endswith('.json'):
                logging.info(f'processing {filename}')
                filestr = (Path(root) / filename).read_text()
                data = json.loads(filestr)
                summary = extract_summary(data)
                forecasts.append(summary)
                htmlfile = get_file_name(data) + '.html'
                summary['href'] = htmlfile
                template_str = Path(template_file).read_text()
                template_str = template_str.replace(
                    '<MODEL>', f'model_data=\n{filestr}')
                with open(str(out_dir / htmlfile), 'w') as outfile:
                    outfile.write(template_str)
    index = json.dumps(forecasts)
    index_str = Path(index_file).read_text()
    index_str = index_str.replace(
        '<MODELINDEX>', f'forecast_index=\n{index}')
    with open(out_dir / 'index.html', 'w') as outfile:
        outfile.write(index_str)


def get_file_name(data):
    undefined = 'UNDEFINED'
    source = ''
    created = undefined
    keys = data.keys()
    if 'source' in keys:
        source = data['source']
    if 'reftime' in keys:
        created = data['created']
        created = datetime.datetime.fromtimestamp(created // 1e3)
        created = str(created).replace(':', '_')
    elif 'created' in keys:
        created = data['created']
        created = datetime.datetime.fromtimestamp(created // 1e3)
        created = str(created).replace(':', '_')
    return f'{source}_{created}'


def extract_summary(data):
    results= {}
    for k in data.keys():
        if k != 'rows':
            results[k] = data[k]
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Generates user-friendly HTML pages to visualize irradiance'
                    ' forecasts made by Apollo models.')
    parser.add_argument(
        '--forecast_path', '-i', type=str,
        default=storage.get('output/json'),
        help='Directory containing JSON forecasts to process.')
    parser.add_argument(
        '--html_path', '-o', type=str,
        default=storage.get('assets/html/solar/forecasts'),
        help='Directory to write the generated HTML files.')
    parser.add_argument(
        '--index_template', '-n', type=str,
        default=storage.get('assets/schemas/html') / 'index.html',
        help='Name of the template file where the model index will be inserted.')
    parser.add_argument(
        '--forecast_template', '-t', type=str,
        default=storage.get('assets/schemas/html') / 'forecast.html',
        help='Name of the template file where the JSON will be inserted.')
    parser.add_argument(
        '--log', type=str, default='INFO',
        choices=('INFO', 'DEBUG', 'WARN', 'ERROR'),
        help='Sets the log level.')

    args = parser.parse_args()

    logging.basicConfig(format='[{asctime}] {levelname}: {message}',
                        style='{', level=args.log)

    config_string = '\n'.join(
        [f'{key}: {val}' for key, val in vars(args).items()])
    logging.info(f'Starting Apollo server with config: \n{config_string}')
    generate_model_index(
        Path(args.forecast_path), Path(args.html_path),
        args.index_template, args.forecast_template)


if __name__ == '__main__':
    main()
