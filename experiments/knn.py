"""
Solar Radiation Prediction with scikit's DecisionTreeRegressor
"""

import os
import numpy as np

from sklearn.neighbors import KNeighborsRegressor
from sklearn.model_selection import KFold, GridSearchCV, cross_validate
from sklearn.externals import joblib

from apollo.datasets import simple_loader


_CACHE_DIR = "../data"  # where the NAM and GA-POWER data resides
_MODELS_DIR = "../models"  # directory where serialized models will be saved
_DEFAULT_TARGET = 'UGA-C-POA-1-IRR'


def make_model_name(target_hour, target_var):
    # creates a unique name for a model that predicts a specific target variable at a specific target hour
    return 'dtree_%shr_%s.model' % (target_hour, target_var)


def save(model, save_dir, target_hour, target_var):
    # serialize the trained model
    name = make_model_name(target_hour, target_var)
    path = os.path.join(save_dir, name)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    joblib.dump(model, path)

    return path


def load(save_dir, target_hour, target_var):
    # logic to load a serialized model
    name = make_model_name(target_hour, target_var)
    path_to_model = os.path.join(save_dir, name)
    if os.path.exists(path_to_model):
        model = joblib.load(path_to_model)
        return model
    else:
        return None


def train(begin_date='2017-01-01 00:00', end_date='2017-12-31 18:00', target_hour=24, target_var=_DEFAULT_TARGET,
          cache_dir=_CACHE_DIR, save_dir=_MODELS_DIR, tune=True, num_folds=3):
    # load data
    X, y = simple_loader.load(start=begin_date, stop=end_date, target_hour=target_hour, target_var=target_var, cache_dir=cache_dir)

    # train model
    if tune:
        grid = GridSearchCV(
            estimator=KNeighborsRegressor(),
            param_grid={
                'n_neighbors': np.arange(3, 25, 2),  # k
                'weights': ['uniform', 'distance'],  # how are neighboring values weighted
            },
            cv=KFold(n_splits=num_folds, shuffle=True),
            scoring='neg_mean_absolute_error',
            return_train_score=False,
            n_jobs=-1,
        )
        grid.fit(X, y)
        print("Grid search completed.  Best parameters found: ")
        print(grid.best_params_)
        model = grid.best_estimator_
    else:
        model = KNeighborsRegressor()
        model = model.fit(X, y)

    # serialize model to a file
    save_location = save(model, save_dir, target_hour, target_var)
    return save_location


def evaluate(begin_date='2017-12-01 00:00', end_date='2017-12-31 18:00', target_hour=24, target_var=_DEFAULT_TARGET,
             cache_dir=_CACHE_DIR, save_dir=_MODELS_DIR, metrics=['neg_mean_absolute_error'], num_folds=3):
    # load hyperparams saved in training step:
    saved_model = load(save_dir, target_hour, target_var)
    if saved_model is None:
        print('WARNING: Evaluating model using default hyperparameters.  '
              'Run `train` before calling `evaluate` to find optimal hyperparameters.')
        hyperparams = dict()
    else:
        hyperparams = saved_model.get_params()

    # Evaluate the classifier
    model = KNeighborsRegressor(**hyperparams)
    X, y = simple_loader.load(start=begin_date, stop=end_date, target_hour=target_hour, target_var=target_var, cache_dir=cache_dir)
    scores = cross_validate(model, X, y, scoring=metrics, cv=num_folds, return_train_score=False, n_jobs=-1)

    # scores is dictionary with keys "test_<metric_name> for each metric"
    mean_scores = dict()
    for metric in metrics:
        mean_scores[metric] = np.mean(scores['test_' + metric])

    return mean_scores


# TODO - need more specs from Dr. Maier
def predict(begin_date, end_date, target_hour=24, target_var=_DEFAULT_TARGET,
            cache_dir=_CACHE_DIR, save_dir=_MODELS_DIR, output_dir='../predictions'):
    # load serialized model
    model_name = make_model_name(target_hour, target_var)
    path_to_model = os.path.join(save_dir, model_name)
    model = load(save_dir, target_hour, target_var)
    if model is None:
        print("You must train the model before making predictions!\nNo serialized model found at '%s'" % path_to_model)
        return None
    data = simple_loader.load(start=begin_date, stop=end_date, target_var=None, cache_dir=cache_dir)[0]

    # write predictions to a file
    reftimes = np.arange(begin_date, end_date, dtype='datetime64[6h]')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    outpath = os.path.join(output_dir, model_name + '.out.csv')
    with open(outpath, 'w') as outfile:
        for idx, data_point in enumerate(data):
            prediction = model.predict([data_point])
            outfile.write("%s,%s\n" % (reftimes[idx], prediction[0]))

    return outpath