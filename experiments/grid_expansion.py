# compare the performance of models that use a 1x1, 3x3, and 5x5 grid

from math import floor
import numpy as np
import pathlib
import pandas as pd
from sklearn.metrics import mean_absolute_error as mae, \
    mean_squared_error as mse, r2_score as r2
from sklearn.model_selection import TimeSeriesSplit, PredefinedSplit

from apollo.models.trees import RandomForest, GradientBoostedTrees


def rmse(y_true, y_pred, **kwargs):
    return mse(y_true, y_pred, **kwargs)**0.5


_default_metrics = (mae, mse, rmse, r2)
_default_target = 'UGABPOA1IRR'


def run(first='2017-01-01', last='2018-12-31',
        metrics=_default_metrics, method='cv',
        folds=5, split_size=0.5, output='./results/grid'):

    print('Grid-Size Experiment')
    output_dir = pathlib.Path(output).resolve()
    target_hours = np.arange(1, 25)

    first = pd.Timestamp(first).floor(freq='6h')
    last = pd.Timestamp(last).floor(freq='6h')

    for shape in [(1, 1), (3, 3), (5, 5)]:
        shape_string = f'{shape[0]}x{shape[1]}'
        rf = RandomForest(
            name=f'rf-{shape_string}', geo_shape=shape,
            target=_default_target, target_hours=target_hours)
        gbt = GradientBoostedTrees(
            name=f'gbt-{shape_string}', geo_shape=shape,
            target=_default_target, target_hours=target_hours)

        if method == 'cv':
            splitter = TimeSeriesSplit(n_splits=folds)
            outpath = pathlib.Path(output_dir / 'cross_val')
        else:
            splitter = TimeSeriesSplit(n_splits=2)
            outpath = pathlib.Path(output_dir / 'split')

        rf_results = rf.validate(first=first, last=last,
                                 metrics=metrics, splitter=splitter,
                                 multioutput='raw_values')
        gbt_results = gbt.validate(first=first, last=last,
                                   metrics=metrics, splitter=splitter,
                                   multioutput='raw_values')

        outpath.mkdir(parents=True, exist_ok=True)
        outpath_rf = outpath / f'rf_{shape_string}.csv'
        outpath_gbt = outpath / f'gbt_{shape_string}.csv'

        results_df = pd.DataFrame(
            index=target_hours, columns=[m.__name__ for m in metrics])
        for metric in rf_results:
            results_df[metric] = rf_results[metric]
        results_df.to_csv(str(outpath_rf), index_label='Target Hour')

        for metric in gbt_results:
            results_df[metric] = gbt_results[metric]
        results_df.to_csv(str(outpath_gbt), index_label='Target Hour')

    print(f'Done.  Wrote results to {output_dir}')
