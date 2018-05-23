from pathlib import Path

import numpy as np
import scipy as sp
import scipy.spatial
import xarray as xr

import dask
from dask import array as da
from dask.distributed import Client

import torch
from torch.utils.data import Dataset as TorchDataset

from apollo.datasets import nam, ga_power


# The latitude and longitude of the solar array.
# NOTE: This is was taken from Google Maps as the lat/lon of the State
# Botanical Garden of Georgia, because that was the nearest I could find.
ATHENS_LATLON = [33.9052058, -83.382608]


# The planar features of the NAM dataset,
# i.e. those where the Z-axis has size 1.
PLANAR_FEATURES = [
    'PRES_SFC',
    'HGT_SFC',
    'HGT_TOA',
    'TMP_SFC',
    'VIS_SFC',
    'UGRD_TOA',
    'VGRD_TOA',
    'DSWRF_SFC',
    'DLWRF_SFC',
]


def find_nearest(data, *points, **kwargs):
    '''Find the indices of `data` nearest to `points`.

    Returns:
        The unraveled indices into `data` of the cells nearest to `points`.
    '''
    n = len(data)
    shape = data[0].shape
    data = np.require(data).reshape(n, -1).T
    points = np.require(points).reshape(-1, n)
    idx = sp.spatial.distance.cdist(points, data, **kwargs).argmin(axis=1)
    return tuple(np.unravel_index(i, shape) for i in idx)


def slice_xy(data, center, shape):
    '''Slice a dataset in the x and y dimensions.

    Arguments:
        data (xr.Dataset):
            The dataset to slice, having dimension coordinates 'y' and 'x' and
            non-dimension coordinates 'lat' and 'lon' labeled by `(y, x)`.
        center ([lat, lon]):
            The latitude and longitude of the center.
        shape ([height, width]):
            The height and width of the selection in grid units.

    Returns:
        subset (xr.Dataset):
            The result of slicing data.
    '''
    # TODO: The `find_nearest` function is a little too clunky.
    latlon = np.stack([data['lat'], data['lon']])
    i, j = find_nearest(latlon, center)[0]  # indices of center cell
    h, w = shape  # desired height and width of the region
    top = i - int(np.ceil(h/2)) + 1
    bottom = i + int(np.floor(h/2)) + 1
    left = j - int(np.ceil(w/2)) + 1
    right = j + int(np.floor(w/2)) + 1
    slice_y = slice(top, bottom)
    slice_x = slice(left, right)
    return data.isel(y=slice_y, x=slice_x)


def extract_temporal_features(data):
    '''Extract temporal features from a dataset.

    Arguments:
        data (xr.Dataset):
            The dataset from which to extract features, having a dimension
            coordinate named 'reftime'.

    Returns:
        time_data (xr.Dataset):
            A dataset with 4 data variables:
                - ``time_of_year_sin``
                - ``time_of_year_cos``
                - ``time_of_day_sin``
                - ``time_of_day_cos``
    '''
    reftime = data['reftime'].astype('float64')

    time_of_year = reftime / 3.1536e+16  # convert from ns to year
    time_of_year_sin = np.sin(time_of_year * 2 * np.pi)
    time_of_year_cos = np.cos(time_of_year * 2 * np.pi)

    time_of_day = reftime / 8.64e+13  # convert from ns to day
    time_of_day_sin = np.sin(time_of_day * 2 * np.pi)
    time_of_day_cos = np.cos(time_of_day * 2 * np.pi)

    return xr.Dataset({
        'reftime': reftime,
        'time_of_year_sin': time_of_year_sin,
        'time_of_year_cos': time_of_year_cos,
        'time_of_day_sin': time_of_day_sin,
        'time_of_day_cos': time_of_day_cos,
    })


class SolarDataset(TorchDataset):
    def __init__(self, start='2017-01-01 00:00', stop='2017-12-31 18:00', *,
            feature_subset=PLANAR_FEATURES, temporal_features=True,
            center=ATHENS_LATLON, geo_shape=(3, 3),
            target='UGA-C-POA-1-IRR', target_hour=24,
            standardize=True, cache_dir='./data'):

        # Create local Dask cluster and connect.
        # This is not required, but doing so adds useful debugging features.
        self.client = Client()

        year = np.datetime64(start).astype(object).year
        stop_year = np.datetime64(stop).astype(object).year
        assert year == stop_year, "start and stop must be same year"

        cache_dir = Path(cache_dir)
        nam_cache = cache_dir / 'NAM-NMM'
        target_cache = cache_dir / 'GA-POWER'

        data = nam.open_range(start, stop, cache_dir=nam_cache)

        if feature_subset:
            data = data[feature_subset]

        if geo_shape:
            data = slice_xy(data, center, geo_shape)

        if standardize:
            d = data.drop(temporal_features) if temporal_features else data
            mean = d.mean()
            std = d.std()
            data = (data - mean) / std

        if temporal_features:
            temporal_data = extract_temporal_features(data)
            data = xr.merge([data, temporal_data])

        if target:
            target_data = ga_power.open_mb007(target, data_dir=target_cache, group=year)
            target_data['reftime'] -= np.timedelta64(target_hour, 'h')
            data = xr.merge([data, target_data], join='inner')
            data = data.set_coords(target)  # NOTE: the target is a coordinate, not data

        self.dataset = data.persist()
        self.target = target

    def __len__(self):
        return len(self.dataset['reftime'])

    def __getitem__(self, index):
        dataset = self.dataset.isel(reftime=index)
        arrays = dataset.data_vars.values()
        arrays = (np.as_array(a) for a in arrays)

        if self.target:
            target = dataset[self.target]
            target = np.asarray(target)
            return (*arrays, target)
        else:
            return (*arrays,)

    def labels(self):
        '''Get the labels of the columns.
        '''
        names = tuple(self.dataset.data_vars)
        if self.target:
            return (*names, self.target)
        else:
            return (*names,)

    def tabular(self):
        '''Get a tabular version of the dataset as a dask array(s).

        Returns:
            x (array of shape (n,m)):
                The input features, where `n` is the number of instances and
                `m` is the number of columns after flattening the features
            y (array of shape (n,)):
                The target array. This is only returned if a target is given
                to the constructor.
        '''
        n = len(self.dataset['reftime'])
        x = self.dataset.data_vars.values()
        x = (a.data for a in x)
        x = (a.reshape(n, -1) for a in x)
        x = np.concatenate(list(x), axis=1)

        if self.target:
            y = self.dataset[self.target]
            y = y.data
            assert y.shape == (n,)
            return x, y
        else:
            return x
