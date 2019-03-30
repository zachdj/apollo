# compare the performance of models that use a 1x1, 3x3, and 5x5 grid

from math import floor
import numpy as np
import pathlib
import pandas as pd
from sklearn.metrics import mean_absolute_error as mae, \
    mean_squared_error as mse, r2_score as r2
from sklearn.model_selection import TimeSeriesSplit
from sklearn.svm import SVR
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.multioutput import MultiOutputRegressor

from apollo.datasets.solar import SolarDataset


def rmse(y_true, y_pred, **kwargs):
    return mse(y_true, y_pred, **kwargs)**0.5


_default_metrics = (mae, mse, rmse, r2)
_default_target = 'UGABPOA1IRR'
_default_target_hours = np.arange(1, 25)


MODELS = {
    'SVR': SVR(**{
        'C': 1.4,
        'epsilon': 0.6,
        'kernel': 'sigmoid',
        'gamma': 0.001
    }),
    'KNN': KNeighborsRegressor(**{
        'n_neighbors': 5,
        'weights': 'distance',
    }),
    'Random Forest': RandomForestRegressor(**{
        'n_estimators': 100,
        'max_depth': 50,
        'min_impurity_decrease': 0.30
    })
}


def run(first='2017-01-01', last='2018-12-31',
        target=_default_target,
        metrics=_default_metrics,
        method='cv', folds=5,
        output='./results/grid_fast'):
    print('Grid-Size Experiment (Fast Version)')
    output_dir = pathlib.Path(output).resolve()

    first = pd.Timestamp(first).floor(freq='6h')
    last = pd.Timestamp(last).floor(freq='6h')

    for shape in [(1, 1), (3, 3), (5, 5)]:
        shape_string = f'{shape[0]}x{shape[1]}'
        print(f'Running experiment for grid size {shape_string}')
        models = {
            'rf': MODELS['Random Forest'],
            'knn': MODELS['KNN'],
            'svm': MultiOutputRegressor(estimator=MODELS['SVR'])
        }

        print('Loading dataset')
        ds = SolarDataset(
            first, last, geo_shape=shape,
            target=target, target_hours=_default_target_hours)
        x, y = ds.tabular()
        x = np.asarray(x)
        y = np.asarray(y)

        if method == 'cv':
            # use a time-series splitter
            splitter = TimeSeriesSplit(n_splits=folds)
            outpath = pathlib.Path(output_dir / f'{target}/cross_val/')
        else:
            splitter = TimeSeriesSplit(n_splits=2)
            outpath = pathlib.Path(output_dir / f'{target}/split')

        outpath = outpath.resolve()
        outpath.mkdir(parents=True, exist_ok=True)

        for model_name in models:
            print(f'Evaluating Model {model_name} using method {method}')
            model = models[model_name]
            evaluations = {metric.__name__: [] for metric in metrics}
            for train_index, test_index in splitter.split(x):
                train_x, train_y = x[train_index], y[train_index]
                test_x, y_true = x[test_index], y[test_index]

                model.fit(train_x, train_y)
                y_pred = model.predict(test_x)

                # compute error metrics for this split
                for metric in metrics:
                    error = metric(y_true, y_pred, multioutput='raw_values')
                    evaluations[metric.__name__].append(error)

            # find mean errors across all splits
            scores = {m: np.mean(np.asarray(evaluations[m]), axis=0)
                      for m in evaluations}

            # output results
            scores_df = pd.DataFrame(index=_default_target_hours,
                                    columns=[m.__name__ for m in metrics])
            for metric in scores:
                scores_df[metric] = scores[metric]

            outfile = outpath / f'{model_name}_{shape_string}.csv'
            print(f'*** Writing results to {outfile} ***')
            scores_df.to_csv(str(outfile), index_label='Target Hour')
