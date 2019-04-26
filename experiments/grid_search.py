''' More thorough grid search over SVM parameters '''
import numpy as np
from sklearn.svm import SVR
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor
from sklearn.model_selection import GridSearchCV

from apollo.datasets.solar import SolarDataset

models = {
    'svr': SVR(),
    'knn': KNeighborsRegressor(),
    'dtree': DecisionTreeRegressor(),
    'rf': RandomForestRegressor(),
    'gbt': XGBRegressor()
}

grids = {
    'svr': {
        'C': np.arange(1000, 3000, 200),
        'gamma': np.arange(1e-5, 1e-4, 1e-5),
        'kernel': ['rbf']
    },
    'knn': {
        'n_neighbors': np.arange(3, 10, 1),
        'weights': ['uniform', 'distance'],
    },
    'dtree': {
        'criterion': ['mse', 'mae'],
        'splitter': ['best', 'random'],
        'max_depth': [10, 15, 20, 25, 30, -1],
        'min_impurity_decrease': np.arange(0.10, 0.30, 0.05)
    },
    'rf': {
        'criterion': ['mae'],
        'n_estimators': np.arange(25, 150, 25),
        'max_depth': [20, 35, 50, 75, -1],
        'min_impurity_decrease': np.arange(0.10, 0.50, 0.1)
    },
    'gbt': {
        'learning_rate': np.arange(0.01, 0.1, 0.01),
        'n_estimators': np.arange(25, 250, 25),
        'max_depth': [5, 10, 20, 25, 50, -1],
    }

}


def run(first='2017-01-01', last='2018-12-31'):
    for model_name in models:
        print(f'Grid search for model {model_name}')
        grid_search = GridSearchCV(
            estimator=models[model_name],
            param_grid=[grids[model_name]],
            scoring='neg_mean_absolute_error',
            n_jobs=-1,
            pre_dispatch=16,
            cv=5,
            error_score=-10000
        )

        dataset = SolarDataset(target='UGABPOA1IRR', target_hours=24,
                           start=first, stop=last)
        x, y = dataset.tabular()
        grid_search.fit(x, y)

        print('Best params:')
        print(grid_search.best_params_)

        print('Best score:')
        print(grid_search.best_score_)

    print('\n\nDone!')


if __name__ == '__main__':
    run()
