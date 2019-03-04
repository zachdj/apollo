import pandas as pd
import pathlib
from sklearn.metrics import mean_absolute_error as mae, mean_squared_error as mse, r2_score

import apollo.models
from apollo.models.base import list_known_models
from apollo.validation import cross_validate, split_validate


def rmse(y_true, y_pred):
    return mse(y_true, y_pred)**0.5


def _is_abstract(cls):
    if not hasattr(cls, "__abstractmethods__"):
        return False  # an ordinary class
    elif len(cls.__abstractmethods__) == 0:
        return False  # a concrete implementation of an abstract class
    else:
        return True  # an abstract class


def run(validation_method='cv'):
    print(f'Performing comparison experiment with evaluation method '
          f'{"cross-validation" if validation_method=="cv" else "train-test split"}...')
    MODELS = {model.__name__: model for model in list_known_models() if not _is_abstract(model)}
    TARGETS = ['UGAAPOA1IRR', 'UGABPOA1IRR', 'UGAEPOA1IRR']
    EVAL_START_DATE = '2017-01-01'
    EVAL_END_DATE = '2018-05-01'
    METRICS = (mae, mse, rmse, r2_score)
    FOLDS = 5
    SPLIT = 0.5

    for target in TARGETS:
        print(f'Working on target {target}...')
        for model_name in MODELS:
            print(f'Evaluating Model {model_name}...')
            cls = MODELS[model_name]
            model = cls(data_kwargs={'target': target})
            if validation_method == 'cv':
                scores = cross_validate(model, EVAL_START_DATE, EVAL_END_DATE, metrics=METRICS, k=FOLDS)
                outpath = pathlib.Path(f'./results/comparison/{target}/cross_val/').resolve()
            else:
                scores = split_validate(model, EVAL_START_DATE, EVAL_END_DATE, metrics=METRICS, test_size=SPLIT)
                outpath = pathlib.Path(f'./results/comparison/{target}/split/').resolve()

            outpath.mkdir(parents=True, exist_ok=True)
            outpath = outpath / f'{model_name}.csv'

            scores.to_csv(str(outpath), index_label='Target Hour')

    print('Done')


if __name__ == '__main__':
    run(validation_method='split')
    run(validation_method='cv')
