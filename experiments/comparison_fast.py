from math import floor
import numpy as np
import pathlib
import pandas as pd
from sklearn.metrics import mean_absolute_error as mae, \
    mean_squared_error as mse, r2_score as r2
from sklearn.model_selection import TimeSeriesSplit

from sklearn.svm import SVR as scikit_SVR
from sklearn.linear_model import LinearRegression as LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor
from sklearn.multioutput import MultiOutputRegressor

from apollo.datasets.solar import SolarDataset


def rmse(y_true, y_pred, **kwargs):
    return mse(y_true, y_pred, **kwargs)**0.5


_default_metrics = (mae, mse, rmse, r2)


MODELS = {
    'Linear Regression': LinearRegression(),
    'Support Vector Regression': scikit_SVR(**{
        'C': 1000,
        'epsilon': 2.0,
        'kernel': 'rbf',
        'gamma': 0.0001
    }),
    # 'KNN': KNeighborsRegressor(**{
    #     'n_neighbors': 5,
    #     'weights': 'distance',
    # }),
    # 'MLP': MLPRegressor(
    #         hidden_layer_sizes=(57, 23),
    #         activation='relu',
    #         solver='adam',
    #         batch_size=100,
    #         learning_rate_init=0.003,
    #         momentum=0.2,
    #         max_iter=500,
    #         random_state=123,
    # ),
    # 'Model Tree': DecisionTreeRegressor(**{
    #     'splitter': 'best',
    #     'max_depth': 20,
    #     'min_impurity_decrease': 0.25
    # }),
    # 'Random Forest': RandomForestRegressor(**{
    #     'n_estimators': 100,
    #     'max_depth': 50,
    #     'min_impurity_decrease': 0.30
    # }),
    # 'GBT': XGBRegressor(**{
    #     'learning_rate': 0.05,
    #     'n_estimators': 200,
    #     'max_depth': 5,
    # })
}


def run(models=tuple(MODELS.keys()), metrics=_default_metrics,
        first='2017-01-01', last='2018-12-31',
        targets=('UGAAPOA1IRR', 'UGABPOA1IRR', 'UGAEPOA1IRR',),
        target_hours=np.arange(1, 25),
        method='cv', folds=5,
        output='./results/comparison_fast'):

    print('Comparison Experiment')

    output_dir = pathlib.Path(output).resolve()

    for target in targets:
        print(f'\n*** Working on target {target} ***\n')
        ds = SolarDataset(first, last, target=target, target_hours=target_hours)
        x, y = ds.tabular()
        x = np.asarray(x)
        y = np.asarray(y)
        for model_name in models:
            print(f'\n** Evaluating Model {model_name} **\n')
            if model_name == 'KNN':
                model = MultiOutputRegressor(estimator=MODELS[model_name], n_jobs=1)
            else:
                model = MultiOutputRegressor(estimator=MODELS[model_name], n_jobs=-1)
            
            if method == 'cv':
                # use a time-series splitter
                splitter = TimeSeriesSplit(n_splits=folds)
                outpath = pathlib.Path(output_dir / f'{target}/cross_val/')
            else:
                splitter = TimeSeriesSplit(n_splits=2)
                outpath = pathlib.Path(output_dir / f'{target}/split')

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

            # average metrics over all folds
            # find mean errors across all splits
            scores = {m: np.mean(np.asarray(evaluations[m]), axis=0) for m in evaluations}

            # output results
            print(f'* Writing results for Model {model_name} using method {method} *')
            scores_df = pd.DataFrame(index=target_hours,
                                     columns=[m.__name__ for m in metrics])
            for metric in scores:
                scores_df[metric] = scores[metric]

            outpath = outpath.resolve()
            outpath.mkdir(parents=True, exist_ok=True)
            outpath = outpath / f'{model_name}.csv'

            scores_df.to_csv(str(outpath), index_label='Target Hour')

    print('Done!')
