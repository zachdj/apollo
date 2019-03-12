# experiments comparing varieties of ML models for solar irradiance prediction

import pathlib

import apollo.models
import experiments.metrics as apollo_metrics
from apollo.models.base import list_known_models
from apollo.validation import cross_validate, split_validate
from experiments.util import is_abstract

'''
TODOs:
- refactor to use model NAMES
- refactor to use metric NAMES
- better argument parsing
'''


def get_model_classes(model_names):
    classes = []
    for cls in list_known_models():
        if cls.__name__ in model_names:
            classes.append(cls)

    return classes


_default_models = tuple([model.__name__ for model in list_known_models() if not is_abstract(model)])


def run(models=_default_models,
        targets=('UGAAPOA1IRR','UGABPOA1IRR','UGAEPOA1IRR',),
        metrics=('MAE', 'MSE', 'RMSE', 'R2'),
        start='2017-01-01', end='2018-12-31', method='cv', folds=5, split=0.5, output='./results'):

    print('Comparison Experiment')
    model_classes = get_model_classes(models)
    metrics = [apollo_metrics.get(metric) for metric in metrics]
    output_dir = pathlib.Path(output).resolve()

    for target in targets:
        print(f'Working on target {target}...')
        for model_cls in model_classes:
            print(f'Evaluating Model {model_cls.__name__}...')
            model = model_cls(name=f'comparison-{model_cls.__name__}', target=target)
            metric_evaluators = [metric.evaluator for metric in metrics]
            if method == 'cv':
                scores = cross_validate(model, start, end, metrics=metric_evaluators, k=folds)
                outpath = pathlib.Path(output_dir / f'{target}/cross_val/').resolve()
            else:
                scores = split_validate(model, start, end, metrics=metric_evaluators, test_size=split)
                outpath = pathlib.Path(output_dir / f'{target}/split/').resolve()

            outpath.mkdir(parents=True, exist_ok=True)
            outpath = outpath / f'{model_cls.__name__}.csv'

            scores.to_csv(str(outpath), index_label='Target Hour')

    print(f'Done.  Wrote results to {output_dir}')
