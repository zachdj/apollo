# forecast evaluation metrics and helper functions to list metrics and give them human-readable names

from sklearn.metrics import mean_absolute_error as mae, mean_squared_error as mse, r2_score


def _rmse(y_true, y_pred):
    return mse(y_true, y_pred)**0.5


class ForecastMetric(object):
    def __init__(self, name, evaluation_function):
        self._name = name
        self._evaluator = evaluation_function

    @property
    def name(self):
        return self._name

    @property
    def evaluator(self):
        return self._evaluator

    def evaluate(self, y_true, y_pred):
        return self.evaluator(y_true, y_pred)


_METRICS = [
    ForecastMetric('MAE', mae),
    ForecastMetric('MSE', mse),
    ForecastMetric('RMSE', _rmse),
    ForecastMetric('R2', r2_score),
]

_METRICS_MAP = {m.name: m for m in _METRICS}


def get(name):
    return _METRICS_MAP[name]


def get_all():
    return _METRICS


def names():
    return _METRICS_MAP.keys()
