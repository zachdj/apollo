# compare the performance of models that use a 1x1, 3x3, and 5x5 grid

import pandas as pd
import pathlib

import experiments.metrics as apollo_metrics
from apollo.models.trees import RandomForest, GradientBoostedTrees
from apollo.validation import cross_validate, split_validate


def run(start='2017-01-01', stop='2018-12-31',
        metrics=('MAE', 'MSE', 'RMSE', 'R2'),
        method='cv', folds=5, split=0.5, output='./results'):

    print('Grid-Size Experiment')
    metrics = [apollo_metrics.get(metric) for metric in metrics]
    output_dir = pathlib.Path(output).resolve()

    start, stop = pd.Timestamp(start), pd.Timestamp(stop)

    for shape in [(1, 1), (3, 3), (5, 5)]:
        shape_string = f'{shape[0]}x{shape[1]}'
        rf = RandomForest(name=f'rf-{shape_string}', geo_shape=shape)
        gbt = GradientBoostedTrees(name=f'gbt-{shape_string}', geo_shape=shape)

        if method == 'cv':
            rf_results = cross_validate(rf, first=start, last=stop,
                                        metrics=metrics, k=folds)
            gbt_results = cross_validate(gbt, first=start, last=stop,
                                         metrics=metrics, k=folds)
            outpath = pathlib.Path(
                output_dir / 'cross_val').resolve()
        else:
            rf_results = split_validate(rf, first=start, last=stop,
                                        metrics=metrics, test_size=split)
            gbt_results = split_validate(gbt, first=start, last=stop,
                                         metrics=metrics, test_size=split)
            outpath = pathlib.Path(
                output_dir / 'split_val').resolve()

        outpath.mkdir(parents=True, exist_ok=True)
        outpath_rf = outpath / f'rf_{shape_string}.csv'
        outpath_gbt = outpath / f'gbt_{shape_string}.csv'

        rf_results.to_csv(str(outpath_rf), index_label='Target Hour')
        gbt_results.to_csv(str(outpath_gbt), index_label='Target Hour')
