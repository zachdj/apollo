"""
Model which simply predicts the mean value for each data point
"""

import os
import numpy as np

from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import KFold, GridSearchCV, cross_validate
import sklearn.metrics
from sklearn.externals import joblib

from apollo.datasets import simple_loader


_CACHE_DIR = "../data"  # where the NAM and GA-POWER data resides
_MODELS_DIR = "../models"  # directory where serialized models will be saved
_DEFAULT_TARGET = 'UGA-C-POA-1-IRR'

# TODO: rewrite this so the evaluate method can take sklearn scorer functions instead of strings
# dictionary mapping metric names to sklearn metrics
METRICS = {
    'explained_variance': sklearn.metrics.explained_variance_score,
    'neg_mean_absolute_error': sklearn.metrics.mean_absolute_error,
    'neg_mean_squared_error': sklearn.metrics.mean_squared_error,
    'neg_mean_squared_log_error': sklearn.metrics.mean_squared_log_error,
    'neg_median_absolute_error': sklearn.metrics.median_absolute_error,
    'r2': sklearn.metrics.r2_score
}


def make_model_name(target_hour, target_var):
    # creates a unique name for a model that predicts a specific target variable at a specific target hour
    return 'mean_%shr_%s.model' % (target_hour, target_var)


def save(mean_val, save_dir, target_hour, target_var):
    # serialize the trained model
    name = make_model_name(target_hour, target_var)
    path = os.path.join(save_dir, name)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    with open(path, 'w') as outfile:
        outfile.write("%0.6f" % mean_val)

    return path


def load(save_dir, target_hour, target_var):
    name = make_model_name(target_hour, target_var)
    path_to_model = os.path.join(save_dir, name)
    if os.path.exists(path_to_model):
        with open(path_to_model, 'r') as infile:
            mean_value = float(infile.readline())
        return mean_value
    else:
        return None


def train(begin_date='2017-01-01 00:00', end_date='2017-12-31 18:00', target_hour=24, target_var=_DEFAULT_TARGET,
          cache_dir=_CACHE_DIR, save_dir=_MODELS_DIR, tune=True, num_folds=3):
    # load data
    X, y = simple_loader.load(start=begin_date, stop=end_date, target_hour=target_hour, target_var=target_var, cache_dir=cache_dir)
    mean_value = np.mean(y)

    # serialize model to a file
    save_location = save(mean_value, save_dir, target_hour, target_var)
    return save_location


def evaluate(begin_date='2017-12-01 00:00', end_date='2017-12-31 18:00', target_hour=24, target_var=_DEFAULT_TARGET,
             cache_dir=_CACHE_DIR, save_dir=_MODELS_DIR, metrics=['neg_mean_absolute_error'], num_folds=3):
    # Evaluate the classifier
    X, y = simple_loader.load(start=begin_date, stop=end_date, target_hour=target_hour, target_var=target_var, cache_dir=cache_dir)

    scores = {}
    for metric in metrics:
        scores[metric] = []

    kf = KFold(n_splits=num_folds, shuffle=True)
    for train_indices, test_indices in kf.split(y):
        train = y[train_indices]
        mean_val = np.mean(train)
        test = y[test_indices]
        predictions = np.ones(len(test))*mean_val
        for metric in metrics:
            scorer = METRICS[metric]
            score = scorer(test, predictions)
            scores[metric].append(score)

    # scores is dictionary with keys "<metric_name> for each metric"
    mean_scores = dict()
    for metric in metrics:
        mean_scores[metric] = np.mean(scores[metric])

    return mean_scores


# TODO - need more specs from Dr. Maier
def predict(begin_date, end_date, target_hour=24, target_var=_DEFAULT_TARGET,
            cache_dir=_CACHE_DIR, save_dir=_MODELS_DIR, output_dir='../predictions'):
    # TODO
    pass
