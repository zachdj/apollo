""" Abstract base class for models that produce solar radiation predictions

Any object conforming to the Predictor API can be used with the prediction CLI (apollo/prediction/__main__.py)

"""

from abc import ABC, abstractmethod
import os
import json
import datetime
import pandas as pd

from apollo import storage


class Predictor(ABC):

    @abstractmethod
    def __init__(self, name, target, target_hours):
        """ Interface for predictors of solar radiation

        Args:
            target (str):
                The name of the variable to target

            target_hours (Iterable[int]):
                The future hours to be predicted.
        """
        super().__init__()
        self.target_hours = target_hours
        self.target = target
        self.name = name
        self.filename = f'{name}_{target_hours[0]}hr-{target_hours[-1]}hr_{target}.model'
        self.models_dir = storage.get('trained_models')

    @abstractmethod
    def save(self):
        """ Serializes this regressor backing this predictor to a file

        Serialization/deserialization are abstract methods because each Predictor might serialize regressors
        differently.
        For example, scikit-learn recommends using joblib to dump trained regressors, whereas most NN packages like
        TF and PyTorch have built-in serialization mechanisms.

        Returns:
            str: location of the serialized regressor.

        """
        pass

    @abstractmethod
    def load(self):
        """ Deserializes a regressor from a file

        Returns:
            object or None: deserialized regressor if a saved regressor is found.  Otherwise, None.

        """
        pass

    @abstractmethod
    def train(self, start, stop, tune, num_folds):
        """ Fits the predictor and saves it to disk

        Trains the predictor to predict `self.target` at each future hour in `self.target_hours` using
        a `SolarDataset` with reftimes between `start` and `stop`.

        Args:
            start (str):
                Timestamp corresponding to the reftime of the first training instance.
            stop (str):
                Timestamp corresponding to the reftime of the final training instance.
            tune (bool):
                If true, perform cross-validated parameter tuning before training.
            num_folds (int):
                The number of folds to use for cross-validated parameter tuning.  Ignored if `tune` == False.

        Returns:
            str: The path to the serialized predictor.

        """
        pass

    @abstractmethod
    def cross_validate(self, start, stop, num_folds, metrics):
        """ Evaluate this predictor using cross validation

        Args:
            start (str):
                Timestamp corresponding to the first reftime of the validation set.
            stop (str):
                Timestamp corresponding to the final reftime of the validation set.
            num_folds (int):
                The number of folds to use.
            metrics (dict):
                Mapping of metrics that should be used for evaluation.  The key should be the metric's name, and the
                value should be a scoring function that implements the metric.

        Returns:
            dict: mapping of metric names to scores.
        """
        pass

    @abstractmethod
    def predict(self, reftime):
        """ Predict future solar irradiance values starting at a given reftime

        The NAM data for the given reftime will be downloaded if it is not cached locally.

        Args:
            reftime (pandas.Timestamp or numpy.datetime64 or datetime.datetime or str):
                The reference time where the prediction will begin

        Returns:
            list: (timestamp, predicted_irradiance) tuple for each hour in `self.target_hours`

        """
        pass

    def write_prediction(self, prediction, summary_dir, output_dir):
        """ Write a prediction generated by the model to a file

        Two files are generated - a summary file and a prediction file.
        The summary file provides meta-data on the prediction made by a model, including the model name, date created,
        start/end dates, and location of the prediction file.  The prediction file contains column metadata and
        the forecasted irradiance values for each target hour.

        TODO: consider adopting a conventional path for summary and output directories

        Args:
            prediction (Iterable<tuple>):
                A series of (timestamp, predicted_irradiance)
            summary_dir (str or path):
                The directory where the summary file should be written
            output_dir (str):
                The directory where the prediction file should be written

        Returns:
            (str, str): Path to summary file, Path to prediction file

        """

        # ensure output directories exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        if not os.path.exists(summary_dir):
            os.makedirs(summary_dir)

        # create path to summary and to resource files
        start_date, stop_date = prediction[0][0], prediction[-1][0]  # assumes the predictions are sorted
        start_date_f = Predictor._format_date(start_date)
        stop_date_f = Predictor._format_date(stop_date)
        summary_filename = f'{self.name}_{self.target}_{start_date_f}_to_{stop_date_f}.summary.json'
        summary_path = os.path.join(summary_dir, summary_filename)
        summary_path = os.path.realpath(summary_path)

        resource_filename = f'{self.name}_{self.target}_{start_date_f}_to_{stop_date_f}.prediction.json'
        resource_path = os.path.join(output_dir, resource_filename)
        resource_path = os.path.realpath(resource_path)

        # contents of the summary file
        summary_dict = {
            'source': self.name,
            'sourcelabel': self.name.replace('_', ' '),
            'site': self.target,
            'created': round(datetime.datetime.utcnow().timestamp())*1000,  # converted to ms
            'start': Predictor._datestring_to_posix(start_date),
            'stop': Predictor._datestring_to_posix(stop_date),
            'resource': resource_path
        }

        # contents of the prediction file
        data = [(Predictor._datestring_to_posix(time), value) for time, value in prediction]
        data_dict = {
            'start': Predictor._datestring_to_posix(start_date),
            'stop': Predictor._datestring_to_posix(stop_date),
            'site': self.target,
            'columns': [
                {
                    'label': 'TIMESTAMP',
                    'units': '',
                    'longname': '',
                    'type': 'datetime'
                },
                {
                    'label': self.target,
                    'units': 'w/m2',
                    'longname': '',
                    'type': 'number'
                },
            ],
            'rows': data
        }

        # write the summary file
        with open(summary_path, 'w') as summary_file:
            json.dump(summary_dict, summary_file, separators=(',', ':'))

        # write the prediction file
        with open(resource_path, 'w') as resource_file:
            json.dump(data_dict, resource_file, separators=(',', ':'))

        return summary_path, resource_path

    @classmethod
    def _datestring_to_posix(cls, date_string):
        timestring = pd.to_datetime(date_string, utc=True).timestamp()
        return int(timestring * 1000)  # convert to milliseconds

    @classmethod
    def _format_date(cls, date_string):
        dt = pd.to_datetime(date_string, utc=True)
        return dt.strftime('%Y-%m-%dT%X')
