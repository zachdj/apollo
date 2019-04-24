# CLI for convenient experiment-running

import argparse
import json
import logging.config
import os

from experiments.comparison import run as run_comparison
from experiments.comparison_fast import run as run_comparison_fast
from experiments.figures import run as generate_figures
from experiments.grid_expansion import run as run_grid_expansion
from experiments.grid_expansion_fast import run as run_grid_fast
from experiments.svm_grid_search import run as run_svm_search

def setup_logging(
    default_path='logging.json',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """ Setup logging configuration """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def parse_kwarg_list(kwarg_list):
    ''' Parses a list of kwargs formatted like "arg=val" '''
    args = {pair[0]:pair[1] for pair in (kwarg.split('=') for kwarg in kwarg_list)}
    for key, val in args.items():
        if ',' in val:
            list_vals = val.split(',')
            list_vals = list(map(lambda v: v.strip(), list_vals))
            args[key] = list_vals
        else:
            # see if the arg can be converted to a numeric type
            try:
                float_val = float(val)
                args[key] = float_val
            except ValueError:
                pass

            try:
                integral_val = int(val)
                args[key] = integral_val
            except ValueError:
                pass

    return args


EXPERIMENTS = {
    'test': lambda: print('Ran test'),
    'comparison': run_comparison,
    'comparison_fast': run_comparison_fast,
    'figures': generate_figures,
    'grid': run_grid_expansion,
    'grid_fast': run_grid_fast,
    'svm_grid_search': run_svm_search,
}


def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        description='Apollo Experiment Runner',
        argument_default=argparse.SUPPRESS,
    )
    # specify the experiment to run
    parser.add_argument('experiment',
                        type=str, choices=list(EXPERIMENTS.keys()),
                        help='The experiment that you would like to run.')

    parser.add_argument('--kwargs',
                        type=str, nargs='*',
                        help='Keyword arguments to pass to the experiment runner'
                             'Should be formatted like '
                             '"--kwargs arg1=val1 arg2=val2 . . ."')
    # parse args
    args = parser.parse_args()
    args = vars(args)

    experiment_runner = EXPERIMENTS[args['experiment']]
    kwargs = parse_kwarg_list(args['kwargs']) if 'kwargs' in args else dict()

    experiment_runner(**kwargs)


if __name__ == '__main__':
    main()