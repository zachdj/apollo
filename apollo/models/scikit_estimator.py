import abc
import numpy as np
import pandas as pd
import pathlib
import pickle

from sklearn.externals import joblib
from sklearn.multioutput import MultiOutputRegressor

from apollo import timestamps
from apollo.datasets.solar import SolarDataset, \
    DEFAULT_TARGET, DEFAULT_TARGET_HOURS
from apollo.models.base import Model


class ScikitModel(Model, abc.ABC):
    ''' Abstract base class for models that use scikit-learn estimators.

    ScikitModels are backed by estimators conforming to the scikit-learn API
    (i.e. having  ``fit`` and ``predict`` methods).
    If the model targets multiple hours, then scikit's
    :class:`sklearn.multioutput.MultiOutputRegressor` is used to train one
    estimator for each target hour.

    The constructor can be passed an arbitrary set of kwargs which can be used
    to customize the behavior of the model
    The ``name`` keyword argument is peeled off and serves as a model's human-
    readable name.  Each of the other kwargs are passed to the
    :class:`SolarDataset constructor <apollo.datasets.solar.SolarDataset>`
    or to the backing estimator, based on name matching.
    '''
    def __init__(self, name=None, **kwargs):
        ''' Initialize a ScikitModel

        Args:
            name (str):
                A human-readable name for the model.
            **kwargs:
                Keyword arguments used to customize data loading and the
                hyperparameters of the underlying scikit-learn estimator.
        '''
        ts = timestamps.utc_timestamp('now')
        self.kwargs = kwargs

        # peel off kwargs corresponding to model hyperparams
        self.model_kwargs = self.default_hyperparams
        for key in self.model_kwargs:
            if key in kwargs:
                self.model_kwargs[key] = kwargs[key]

        self.data_args = {key: val for key, val in kwargs.items()
                          if key not in self.model_kwargs}

        self.model = None

        self._name = name or f'{self.__class__.__name__}@{ts.isoformat()}'

    @property
    @abc.abstractmethod
    def estimator(self):
        ''' Estimator that conforms to the scikit-learn API
        '''
        pass

    @property
    @abc.abstractmethod
    def default_hyperparams(self):
        ''' Default hyperparameters to use with this model's estimator
        '''
        pass

    @property
    def name(self):
        return self._name

    @property
    def target(self):
        return self.data_args['target'] \
            if 'target' in self.data_args \
            else DEFAULT_TARGET

    @property
    def target_hours(self):
        if 'target_hours' in self.data_args:
            try:
                return tuple(self.data_args['target_hours'])
            except TypeError:
                return self.data_args['target_hours'],
        else:
            try:
                return tuple(DEFAULT_TARGET_HOURS)
            except TypeError:
                return DEFAULT_TARGET_HOURS,

    @classmethod
    def load(cls, path):
        with open(path / 'kwargs.pickle', 'rb') as args_file:
            kwargs = pickle.load(args_file)
        model = cls(**kwargs)
        model.model = joblib.load(path / 'regressor.joblib')

        return model

    def save(self, path):
        if not self.model:
            raise ValueError('Model has not been trained. Ensure `model.fit`'
                             ' is called before `model.save`.')

        # serialize the trained scikit-learn model
        joblib.dump(self.model, path / 'regressor.joblib')

        # serialize kwargs
        kwargs = dict({'name': self.name}, **self.kwargs)
        with open(path / 'kwargs.pickle', 'wb') as outfile:
            pickle.dump(kwargs, outfile)

    def fit(self, first, last):
        ds = SolarDataset(first, last, **self.data_args)
        x, y = ds.tabular()
        x = np.asarray(x)
        y = np.asarray(y)
        self.estimator.set_params(**self.model_kwargs)
        model = MultiOutputRegressor(estimator=self.estimator, n_jobs=1)
        model.fit(x, y)
        self.model = model
        # save standardization parameters
        self.data_args['standardize'] = (ds.mean, ds.std)

    def forecast(self, reftime):
        target = self.target
        target_hours = self.target_hours

        # prevent SolarDataset from trying to load targets
        data_args = dict(self.data_args)
        data_args['target'] = None

        reftime = timestamps.utc_timestamp(reftime)

        ds = SolarDataset(reftime, reftime + pd.Timedelta(6, 'h'), **data_args)
        x = np.asarray(ds.tabular())
        y = self.model.predict(x)[0]
        index = [reftime + pd.Timedelta(1, 'h') * n for n in target_hours]
        df = pd.DataFrame(y, index=pd.DatetimeIndex(index), columns=[target])
        return df
