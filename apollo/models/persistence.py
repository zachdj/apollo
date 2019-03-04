import abc
import numpy as np
import pandas as pd
import pathlib
import pickle

from apollo.datasets.ga_power import open_sqlite
from apollo.models.base import ValidatableModel


class PersistenceModel(ValidatableModel):
    ''' Predicts solar irradiance at time T using the irradiance reading at time (T - 24 hours) '''
    def __init__(self, data_kwargs=None, model_kwargs=None, **kwargs):
        ''' Initialize a PersistanceModel

        Args:
            data_kwargs (dict or None):
                kwargs to be passed to the SolarDataset constructor
            model_kwargs (dict or None):
                kwargs used to specify model behavior
            **kwargs:
                other kwargs used for model initialization, such as model name
        '''
        ts = pd.Timestamp('now')
        data_kwargs = data_kwargs or {}
        default_data_kwargs = {
            'lag': 0,
            'target': 'UGABPOA1IRR',
            'target_hours': tuple(np.arange(1, 25)),
            'standardize': True
        }
        # self.data_kwargs will be a merged dictionary with values from `data_kwargs` replacing default values
        self.data_kwargs = {**default_data_kwargs, **data_kwargs}

        self.model_kwargs = model_kwargs or {}

        self.kwargs = kwargs
        self._name = f'PersistenceModel@{ts.isoformat()}'
        if 'name' in kwargs:
            self._name = kwargs['name']

    @property
    def name(self):
        return self._name

    @property
    def target(self):
        return self.data_kwargs['target']

    @property
    def target_hours(self):
        return tuple(self.data_kwargs['target_hours'])

    @classmethod
    def load(cls, path):
        name = path.name
        with open(path / 'data_args.pickle', 'rb') as data_args_file:
            data_kwargs = pickle.load(data_args_file)
        with open(path / 'kwargs.pickle', 'rb') as kwargs_file:
            kwargs = pickle.load(kwargs_file)
        model = cls(name=name, data_kwargs=data_kwargs, model_kwargs=None, **kwargs)

        return model

    def save(self, path):
        # serialize kwargs
        with open(path / 'data_args.pickle', 'wb') as outfile:
            pickle.dump(self.data_kwargs, outfile)
        with open(path / 'kwargs.pickle', 'wb') as outfile:
            pickle.dump(self.kwargs, outfile)

    def fit(self, first, last):
        pass

    def forecast(self, reftime):
        forecast_reach = max(*self.data_kwargs['target_hours'])  # maximum forecast hour
        past_values_start = pd.Timestamp(reftime) - pd.Timedelta(forecast_reach, 'h')
        past_values_end = pd.Timestamp(reftime)
        past_values = open_sqlite(self.data_kwargs['target'], start=past_values_start, stop=past_values_end)

        index = [reftime + pd.Timedelta(1, 'h') * n for n in self.data_kwargs['target_hours']]
        predictions = []
        for timestamp in index:
            past_timestamp = timestamp - pd.Timedelta(24, 'h')
            past_val = past_values[past_timestamp]
            predictions.append(past_val)

        df = pd.DataFrame(predictions, index=pd.DatetimeIndex(index), columns=[self.data_kwargs['target']])
        return df
