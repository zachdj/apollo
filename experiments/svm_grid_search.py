''' More thorough grid search over SVM parameters '''
import numpy as np
from sklearn.svm import SVR
from sklearn.model_selection import GridSearchCV

from apollo.datasets.solar import SolarDataset

coarse_param_grid = [
    {'C': [1, 10, 100, 1000], 'kernel': ['linear']},
    {'C': [1, 10, 100, 1000], 'gamma': [0.01, 0.001, 0.0001], 'kernel': ['rbf']},
]

narrow_param_grid = [
    {
        'C': np.arange(1000, 3000, 200),
        'gamma': np.arange(1e-5, 1e-4, 1e-5),
        'kernel': ['rbf']
    },
]


def run(first='2017-01-01', last='2018-12-31'):
    grid_search = GridSearchCV(
        estimator=SVR(),
        param_grid=narrow_param_grid,
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


if __name__ == '__main__':
    run()
