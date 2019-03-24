# experiments comparing varieties of ML models for solar irradiance prediction

from math import floor
import numpy as np
import pathlib
import pandas as pd
from sklearn.metrics import mean_absolute_error as mae, \
    mean_squared_error as mse, r2_score as r2
from sklearn.model_selection import TimeSeriesSplit, PredefinedSplit

import apollo.models
from apollo.models.base import list_known_models
from experiments.util import is_abstract


def rmse(y_true, y_pred, **kwargs):
    return mse(y_true, y_pred, **kwargs)**0.5


def get_model_classes(model_names):
    classes = []
    for cls in list_known_models():
        if cls.__name__ in model_names:
            classes.append(cls)

    return classes


_default_models = tuple([model.__name__ for model in list_known_models()
                         if not is_abstract(model)])
_default_metrics = (mae, mse, rmse, r2)


def run(models=_default_models, metrics=_default_metrics,
        targets=('UGAAPOA1IRR', 'UGABPOA1IRR', 'UGAEPOA1IRR',),
        first='2017-01-01', last='2018-12-31',
        method='cv', folds=5, split_size=0.5, output='./results'):

    print('Comparison Experiment')
    model_classes = get_model_classes(models)
    output_dir = pathlib.Path(output).resolve()
    target_hours = np.arange(1, 25)

    for target in targets:
        print(f'Working on target {target}...')
        for model_cls in model_classes:
            print(f'Evaluating Model {model_cls.__name__}...')
            model = model_cls(name=f'comparison-{model_cls.__name__}',
                              target=target, target_hours=target_hours)
            if method == 'cv':
                # use a time-series splitter
                splitter = TimeSeriesSplit(n_splits=folds)
                outpath = pathlib.Path(output_dir / f'{target}/cross_val/')
            else:
                # create custom splitter with PredefinedSplit
                test_pct = max(0, min(split_size, 1))
                first = pd.Timestamp(first).floor(freq='6h')
                last = pd.Timestamp(last).floor(freq='6h')
                # total reftimes in the selected dataset
                reftime_count = (last - first) // pd.Timedelta(6, 'h')

                # create index for PredefinedSplit
                testing_count = floor(reftime_count * test_pct)
                training_count = reftime_count - testing_count
                test_fold = np.concatenate((
                    np.ones(training_count) * -1,  # -1 indicates training set
                    np.zeros(testing_count)  # 0 indicates testing set, 1st fold
                ))
                splitter = PredefinedSplit(test_fold)
                outpath = pathlib.Path(output_dir / f'{target}/split/')

            scores = model.validate(first=first, last=last,
                                    splitter=splitter, metrics=metrics,
                                    multioutput='raw_values')

            # scores will be a dictionary mapping metric names to computed vals
            scores_df = pd.DataFrame(index=target_hours,
                                     columns=[m.__name__ for m in metrics])
            for metric in scores:
                scores_df[metric] = scores[metric]

            outpath = outpath.resolve()
            outpath.mkdir(parents=True, exist_ok=True)
            outpath = outpath / f'{model_cls.__name__}.csv'

            scores_df.to_csv(str(outpath), index_label='Target Hour')

    print(f'Done.  Wrote results to {output_dir}')
