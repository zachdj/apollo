#!/usr/bin/env python3
'''A NAM dataset loader.

> The North American Mesoscale Forecast System (NAM) is one of the
> major weather models run by the National Centers for Environmental
> Prediction (NCEP) for producing weather forecasts. Dozens of weather
> parameters are available from the NAM grids, from temperature and
> precipitation to lightning and turbulent kinetic energy.

> As of June 20, 2006, the NAM model has been running with a non-
> hydrostatic version of the Weather Research and Forecasting (WRF)
> model at its core. This version of the NAM is also known as the NAM
> Non-hydrostatic Mesoscale Model (NAM-NMM).

Most users will be interested in the `load` function which loads
the data for a single NAM-NMM run at a particular reference time,
downloading and preprocessing GRIB files if needed. The actual data
loading logic is encapsulated in the `NAMLoader` class which can be
used for finer grain control over the preprocessing and file system
usage or to load different NAM datasets like NAM-ANL.

The data loading logic works like this:

1. If a netCDF file exists for the dataset, it is loaded immediately
   without any preprocessing.
2. Otherwise any GRIB files required for building the dataset are
   downloaded if they do not already exist.
3. The data is then extracted from the GRIBs. The raw data is subsetted
   to an area encompasing Georgia, and only a subset of the features
   are extracted. The level and time axes are reconstructed from
   multiple GRIB features.
4. The dataset is then saved to a netCDF file, and the GRIB files are
   removed.

The dataset is returned as an `xarray.Dataset`, and each variable has
exactly five dimensions: reftime, forecast, z, y, and x. The z-axis for
each variable has a different name depending on the type of index
measuring the axis, e.g. `heightAboveGround` for height above the
surface in meters or `isobaricInhPa` for isobaric layers. The names of
the variables follow the pattern `FEATURE_LAYER` where `FEATURE` is a
short identifier for the feature being measured and `LAYER` is the type
of z-axis used by the variable, e.g. `t_isobaricInhPa` for the
temperature at the isobaric layers.

This module exposes several globals containing general metadata about
the NAM dataset.
'''

from datetime import datetime, timedelta, timezone
from itertools import groupby
from logging import getLogger
from pathlib import Path
from time import sleep

import cartopy.crs as ccrs
import cartopy.feature as cf
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import scipy as sp
import scipy.spatial.distance
import pygrib
import requests
import xarray as xr

logger = getLogger(__name__)

# URLs of remote grib files.
# PROD_URL typically has the most recent 7 days.
# ARCHIVE_URL typically has the most recent 11 months, about 1 week behind.
PROD_URL = 'http://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.{ref.year:04d}{ref.month:02d}{ref.day:02d}/nam.t{ref.hour:02d}z.awphys{forecast:02d}.tm00.grib2'
ARCHIVE_URL = 'https://nomads.ncdc.noaa.gov/data/meso-eta-hi/{ref.year:04d}{ref.month:02d}/{ref.year:04d}{ref.month:02d}{ref.day:02d}/nam_218_{ref.year:04d}{ref.month:02d}{ref.day:02d}_{ref.hour:02d}00_{forecast:03d}.grb'
ARCHIVE_URL2 = 'https://nomads.ncdc.noaa.gov/data/meso-eta-hi/{ref.year:04d}{ref.month:02d}/{ref.year:04d}{ref.month:02d}{ref.day:02d}/nam_218_{ref.year:04d}{ref.month:02d}{ref.day:02d}_{ref.hour:02d}00_{forecast:03d}.grb2'

# The default file name formats for local grib and cdf datasets.
LOCAL_GRIB_FMT = 'nam.{ref.year:04d}{ref.month:02d}{ref.day:02d}/nam.t{ref.hour:02d}z.awphys{forecast:02d}.tm00.grib2'
LOCAL_CDF_FMT = 'nam.{ref.year:04d}{ref.month:02d}{ref.day:02d}/nam.t{ref.hour:02d}z.awphys.tm00.nc'

# The forecast period of the NAM-NMM dataset.
FORECAST_PERIOD = tuple(range(0, 36)) + tuple(range(36, 85, 3))

# The default features to extract from the gribs.
# A list of feature abreviations.
DEFAULT_FEATURES = ['dlwrf', 'dswrf', 'pres', 'vis', 'tcc', 't', 'r', 'u', 'v', 'w']

# The default geographic subset to extract from the gribs.
# Given as the kwargs to `NAMLoader.latlon_subset`.
DEFAULT_GEO = {
    'center': (32.8, -83.6),
    'apo': 50,
}

# The projection of the NAM-NMM dataset as a `cartopy.crs.CRS`.
# Useful for programatically reasoning about the projection.
PROJ = ccrs.LambertConformal(
    central_latitude=25,
    central_longitude=265,
    standard_parallels=(25, 25),

    # The default cartopy globe is WGS 84, but
    # NAM assumes a spherical globe with radius 6,371.229 km
    globe=ccrs.Globe(ellipse=None, semimajor_axis=6371229, semiminor_axis=6371229),
)

# The projection of the NAM-NMM dataset as a CF convention grid mapping.
# Stored in the netCDF files when they are converted.
CF_PROJ = {
    'grid_mapping_name': 'lambert_conformal_conic',
    'latitude_of_projection_origin': 25.0,
    'longitude_of_central_meridian': 265.0,
    'standard_parallel': 25.0,
    'earth_radius': 6371229.0,
}

# A description of features in the NAM-NMM dataset.
# Maps feature abreviations to their descriptions.
FEATURES = {
    '10u':    '10 metre U wind component',
    '10v':    '10 metre V wind component',
    '2d':     '2 metre dewpoint temperature',
    '2r':     'Surface air relative humidity',
    '2t':     '2 metre temperature',
    '4lftx':  'Best (4-layer) lifted index',
    'absv':   'Absolute vorticity',
    'acpcp':  'Convective precipitation (water)',
    'al':     'Albedo',
    'bmixl':  'Blackadar mixing length scale',
    'cape':   'Convective available potential energy',
    'cd':     'Drag coefficient',
    'cfrzr':  'Categorical freezing rain',
    'ci':     'Sea-ice cover',
    'cicep':  'Categorical ice pellets',
    'cin':    'Convective inhibition',
    'cnwat':  'Plant canopy surface water',
    'crain':  'Categorical rain',
    'csnow':  'Categorical snow',
    'dlwrf':  'Downward long-wave radiation flux',
    'dswrf':  'Downward short-wave radiation flux',
    'fricv':  'Frictional velocity',
    'gh':     'Geopotential Height',
    'gust':   'Wind speed (gust)',
    'hindex': 'Haines Index',
    'hlcy':   'Storm relative helicity',
    'hpbl':   'Planetary boundary layer height',
    'lftx':   'Surface lifted index',
    'lhtfl':  'Latent heat net flux',
    'lsm':    'Land-sea mask',
    'ltng':   'Lightning',
    'maxrh':  'Maximum relative humidity',
    'minrh':  'Minimum Relative Humidity',
    'mslet':  'MSLP (Eta model reduction)',
    'mstav':  'Moisture availability',
    'orog':   'Orography',
    'pli':    'Parcel lifted index (to 500 hPa)',
    'poros':  'Soil porosity',
    'pres':   'Pressure',
    'prmsl':  'Pressure reduced to MSL',
    'pwat':   'Precipitable water',
    'q':      'Specific humidity',
    'r':      'Relative humidity',
    'refc':   'Maximum/Composite radar reflectivity',
    'refd':   'Derived radar reflectivity',
    'rlyrs':  'Number of soil layers in root zone',
    'sde':    'Snow depth',
    'sdwe':   'Water equivalent of accumulated snow depth',
    'shtfl':  'Sensible heat net flux',
    'slt':    'Soil type',
    'smdry':  'Direct evaporation cease (soil moisture)',
    'smref':  'Transpiration stress-onset (soil moisture)',
    'snowc':  'Snow cover',
    'soill':  'Liquid volumetric soil moisture (non-frozen)',
    'soilw':  'Volumetric soil moisture content',
    'sp':     'Surface pressure',
    'sr':     'Surface roughness',
    'ssw':    'Soil moisture content',
    'st':     'Soil Temperature',
    't':      'Temperature',
    'tcc':    'Total Cloud Cover',
    'tke':    'Turbulent kinetic energy',
    'tmax':   'Maximum temperature',
    'tmin':   'Minimum temperature',
    'tp':     'Total Precipitation',
    'u':      'U component of wind',
    'ulwrf':  'Upward long-wave radiation flux',
    'uswrf':  'Upward short-wave radiation flux',
    'v':      'V component of wind',
    'veg':    'Vegetation',
    'vgtyp':  'Vegetation Type',
    'vis':    'Visibility',
    'VRATE':  'Ventilation Rate',
    'vucsh':  'Vertical u-component shear',
    'vvcsh':  'Vertical v-component shear',
    'w':      'Vertical velocity',
    'wilt':   'Wilting Point',
    'wz':     'Geometric vertical velocity',
}


def load(ref_time=None, data_dir='.', url_fmt=None, save_netcdf=True, keep_gribs=False):
    '''Load a NAM-NMM dataset for the given reference time.

    Args:
        ref_time (datetime or str):
            The reference time of the data set. It is rounded down to the
            previous model run. It may be given as a string with the format
            '%Y%m%d %H%M'. The default is the current time.
        data_dir (path-like):
            The base path for the dataset.
        url_fmt (string):
            The format for GRIB URLs. It uses the keys `ref` and `forecast`
            for the reference time and forecast hour respectively. The
            default is either the production URL from NCEP or the archive
            URL from NCDC depending on the reference time.
        save_netcdf (bool):
            If true, save the dataset to a netCDF file.
            This argument is ignored if loading from netCDF.
        keep_gribs (bool):
            If true, the GRIB files are not deleted.
            This argument is ignored if loading from netCDF.
    '''
    loader = NAMLoader(ref_time=ref_time, data_dir=data_dir, url_fmt=url_fmt)
    return loader.load(save_netcdf=save_netcdf, keep_gribs=keep_gribs)


def reftime(ds, tz=None):
    '''Returns the first value along the reftime dimension as a native datetime.

    Example:
        Get the third value along the reftime dimension
        ```
        nam.reftime(ds.isel(reftime=2))
        ```

    Args:
        tz (timezone):
            The data is converted to this timezone.
            The default is eastern standard time.
    '''
    if not tz:
        tz = timezone(timedelta(hours=-5), name='US/Eastern')

    reftime = (ds.reftime.data[0]
        .astype('datetime64[ms]')     # truncate from ns to ms (lossless for NAM data)
        .astype('O')                  # convert to native datetime
        .replace(tzinfo=timezone.utc) # set timezone
        .astimezone(tz))              # convert to given timezone

    return reftime


def latlon_index(lats, lons, pos):
    '''Find the index of the cell nearest to `pos`.

    Args:
        lats (2d array):
            The latitudes for each cell of the grid.
        lons (2d array):
            The longitudes for each cell of the grid.
        pos (float, float):
            The position as a `(latitude, longitude)` pair.

    Returns (int, int):
        The index of the cell nearest to `pos`.
    '''
    latlons = np.stack((lats.flatten(), lons.flatten()), axis=-1)
    target = np.array([pos])
    dist = sp.spatial.distance.cdist(target, latlons)
    am = np.argmin(dist)
    i, j = np.unravel_index(am, lats.shape)
    return i, j


def latlon_subset(lats, lons, center, apo):
    '''Build a slice to subset projected data based on lats and lons.

    Example:
        Subset projected data given the lats and lons:
        ```
        data, lats, lons = ...
        subset = latlon_subset(lats, lons)
        data[subset], lats[subset], lons[subset]
        ```

    Args:
        lats (2d array):
            The latitudes for each cell of the grid.
        lons (2d array):
            The longitudes for each cell of the grid.
        center (float, float):
            The center of the subset as a `(latitude, longitude)` pair.
        apo (int):
            The apothem of the subset in grid units.
            I.e. the number of cells from the center to the edge.

    Returns (slice, slice):
        A pair of slices characterizing the subset.
    '''
    i, j = latlon_index(lats, lons, center)
    return slice(i - apo, i + apo + 1), slice(j - apo, j + apo + 1)


def show(data):
    '''A helper to plot NAM data.

    Example:
        Plot the 0-hour forecast of surface temperature:
        ```
        ds = nam.load()
        nam.show(ds.t_surface.isel(reftime=0, forecast=0))
        ```
    '''
    state_boarders = cf.NaturalEarthFeature('cultural', 'admin_1_states_provinces_lines', '50m', facecolor='none')
    ax = plt.subplot(projection=PROJ)
    ax.add_feature(state_boarders, edgecolor='black')
    ax.add_feature(cf.COASTLINE)
    data.plot(ax=ax)
    plt.show(block=False)


class NAMLoader:
    '''A data loader for the NAM-NMM dataset.

    The main method is `load` which is equivalent to the module-level function
    of the same name with the additional functionality of controling the
    feature list and grid subset in the preprocessing step.

    The `load` method is built from a pipeline of the `download`, `unpack`, and
    `repack` methods. `download` ensures that the GRIB files exist locally,
    `unpack` extracts a subset of the data, and `repack` recombines the data
    into an `xarray.Dataset` which can be serialized to a netCDF file. Custom
    preprocessing pipelines can be built from these methods.

    Additionally, this class includes helpers to extract lat-lon grids from the
    GRIB files, compute grid subsets, and access the files of the dataset.
    '''

    def __init__(self,
            ref_time=None,
            data_dir='.',
            url_fmt=None,
            local_grib_fmt=LOCAL_GRIB_FMT,
            local_cdf_fmt=LOCAL_CDF_FMT):
        '''Creates a NAM data loader for the given reference time.

        Args:
            ref_time (datetime):
                The reference time of the data set. It is rounded down to the
                previous model run. It may be given as a string with the format
                '%Y%m%d %H%M'. The default is the current time.
            data_dir (Path):
                The base path for the dataset.
            url_fmt (string):
                The format for GRIB URLs. It uses the keys `ref` and `forecast`
                for the reference time and forecast hour respectively. The
                default is either the production URL from NCEP or the archive
                URL from NCDC depending on the reference time.
            local_grib_fmt (string):
                The format for local grib file names.
            local_cdf_fmt (string):
                The format for local netCDF file names.
        '''
        now = datetime.now(timezone.utc)

        # The reference time must be in UTC
        if not ref_time:
            ref_time = now
        elif isinstance(ref_time, str):
            ref_time = datetime.strptime(ref_time, '%Y%m%d %H%M').astimezone(timezone.utc)
        else:
            ref_time = ref_time.astimezone(timezone.utc)

        # The reference time is rounded to the previous 0h, 6h, 12h, or 18h
        ref_time = ref_time.replace(
            hour=(ref_time.hour // 6) * 6,
            minute=0,
            second=0,
            microsecond=0, )

        # The default url_fmt is based on the reference time.
        if not url_fmt:
            days_delta = (now - ref_time).days
            if days_delta > 7:
                if ref_time < datetime(year=2017, month=4, day=1, tzinfo=timezone.utc):
                    url_fmt = ARCHIVE_URL
                else:
                    url_fmt = ARCHIVE_URL2
            else:
                url_fmt = PROD_URL

        self.ref_time = ref_time
        self.data_dir = Path(data_dir)
        self.url_fmt = url_fmt
        self.local_grib_fmt = local_grib_fmt
        self.local_cdf_fmt = local_cdf_fmt

    def load(self,
            features=None,
            geo=None,
            save_netcdf=True,
            keep_gribs=False,
            force_download=False):
        '''Load the dataset, downloading and preprocessing GRIBs as necessary.

        If the dataset exists as a local netCDF file, it is loaded and
        returned. Otherwise, any missing GRIB files are downloaded and
        preprocessed into an xarray Dataset. The dataset is then saved as a
        netCDF file, the GRIBs are deleted, and the dataset is returned.

        Args:
            features (list of str):
                Filter the dataset to only include these features.
                Defaults to `nam.DEFAULT_FEATURES`.
                This argument is ignored if loading from netCDF.
            geo (slice, slice) or (dict):
                Reduce the variables to this geographic subset.
                The subset is given in the form `(y, x)` where `y` and `x` are
                slices for their respective dimensions. If given as a dict, it
                is forwarded as the kwargs to `self.latlon_subset`.
                Defaults to `nam.DEFAULT_GEO`.
                This argument is ignored if loading from netCDF.
            save_netcdf (bool):
                If true, save the dataset to a netCDF file.
                This argument is ignored if loading from netCDF.
            keep_gribs (bool):
                If true, the GRIB files are not deleted.
                This argument is ignored if loading from netCDF.
            force_download (bool):
                Force the download and processing of grib files, ignoring any
                existing grib or netCDF datasets.
        '''
        if not features:
            features = DEFAULT_FEATURES

        if not geo:
            geo = DEFAULT_GEO

        # If the dataset already exists, just load it.
        # Otherwise, download and convert gribs.
        # TODO: verify that the data matches the requested feature/geo subsets
        if self.local_cdf.exists() and not force_download:
            data = xr.open_dataset(str(self.local_cdf))
        else:
            self.download(force=force_download)
            data = self.unpack(features, geo)
            data = self.repack(data)

        # Save as netCDF.
        if save_netcdf:
            data.to_netcdf(str(self.local_cdf))

        # Delete the gribs.
        if not keep_gribs:
            for grib_path in self.local_gribs:
                grib_path.unlink()

        return data

    def download(self, force=False):
        '''Download the missing GRIB files for this dataset.

        Args:
            force (bool):
                Download the GRIB files even if they already exists locally.
        '''
        for path, url in zip(self.local_gribs, self.remote_gribs):
            path.parent.mkdir(exist_ok=True)

            # No need to download if we already have the file.
            # TODO: Can we check that the file is valid before skipping it?
            if path.exists() and not force:
                continue

            # Attempt download.
            # In case of error, retry with exponential backoff.
            # Total time increases with max_tries:
            #    8 tries  =   8.5   minutes
            #    9 tries  =  17.033 minutes
            #   10 tries  =  34.1   minutes
            max_tries = 8
            timeout = 10  # the servers are kinda slow
            for i in range(max_tries):
                try:
                    with path.open('wb') as fd:
                        logger.info('Downloading {}'.format(url))
                        r = requests.get(url, timeout=timeout, stream=True)
                        r.raise_for_status()
                        for chunk in r.iter_content(chunk_size=128):
                            fd.write(chunk)
                    break
                except IOError as err:
                    # IOError includes both system and HTTP errors.
                    path.unlink()
                    logger.warn(err)
                    if i != max_tries - 1:
                        delay = 2**i
                        logger.warn('Download failed, retrying in {}s'.format(delay))
                        sleep(delay)
                    else:
                        logger.error('Download failed, giving up')
                        raise err
                except (Exception, SystemExit, KeyboardInterrupt) as err:
                    # Delete partial file in case of keyboard interupt etc.
                    path.unlink()
                    raise err

    def unpack(self, features=None, geo=None):
        '''Unpacks and subsets the local GRIB files.

        Args:
            features (list of str):
                If not None, filter the dataset to only include these features.
            geo (slice, slice) or (dict):
                If not None, reduce the variables to this geographic subset.
                The subset is given in the form `(y, x)` where `y` and `x` are
                slices for their respective dimensions. If given as a dict, the
                subset is the result of forwarding this as the kwargs to
                `self.latlon_subset`.

        Returns:
            A list of `DataArray`s for each feature in the GRIB files.
        '''
        if isinstance(geo, dict):
            geo = self.latlon_subset(**geo)

        variables = []
        for path in self.local_gribs:
            logger.info('Processing {}'.format(path))
            grbs = pygrib.open(str(path))
            if features:
                grbs = grbs.select(shortName=features)
            for g in grbs:

                layer_type = g.typeOfLevel
                if layer_type == 'unknown':
                    layer_type = 'z' + str(g.typeOfFirstFixedSurface)

                name = '_'.join([g.shortName, layer_type])

                ref_time = datetime(g.year, g.month, g.day, g.hour, g.minute, g.second)
                ref_time = np.datetime64(ref_time)

                forecast = np.timedelta64(g.forecastTime, 'h')

                lats, lons = g.latlons()  # lats and lons are in (y, x) order
                if geo:
                    lats, lons = lats[geo], lons[geo]  # subset applies to (y, x) order
                lats, lons = np.copy(lats), np.copy(lons)  # release reference to the grib
                y, x = self.proj_coords(lats, lons) # convert to projected coordinates

                values = g.values  # values are in (y, x) order
                if geo:
                    values = values[geo]  # subset applies to (y, x) order
                values = np.copy(values)  # release reference to the grib
                values = np.expand_dims(values, 0)  # (z, y, x)
                values = np.expand_dims(values, 0)  # (forecast, z, y, x)
                values = np.expand_dims(values, 0)  # (reftime, forecast, z, y, x)

                attrs = {
                    'standard_name': g.cfName or g.name.replace(' ', '_').lower(),
                    'short_name': g.shortName,
                    'layer_type': layer_type,
                    'units': g.units,
                    'grid_mapping': 'NAM218',
                }

                dims = ['reftime', 'forecast', layer_type, 'y', 'x']

                coords = {
                    'NAM218': ([], 0, CF_PROJ),
                    'lat': (('y', 'x'), lats, {
                        'standard_name': 'latitude',
                        'long_name': 'latitude',
                        'units': 'degrees_north',
                    }),
                    'lon': (('y', 'x'), lons, {
                        'standard_name': 'longitude',
                        'long_name': 'longitude',
                        'units': 'degrees_east',
                    }),
                    'reftime': ('reftime', [ref_time], {
                        'standard_name': 'forecast_reference_time',
                        'long_name': 'reference time',
                        # # units and calendar are handled automatically by xarray
                        # 'units': 'seconds since 1970-01-01 0:0:0',
                        # 'calendar': 'standard',
                    }),
                    'forecast': ('forecast', [forecast], {
                        'standard_name': 'forecast_period',
                        'long_name': 'forecast period',
                        'axis': 'T',
                        # # units and calendar are handled automatically by xarray
                        # 'units': 'seconds',
                    }),
                    layer_type: (layer_type, [g.level], {
                        'units': g.unitsOfFirstFixedSurface,
                        'axis': 'Z',
                    }),
                    'y': ('y', y, {
                        'standard_name': 'projection_y_coordinate',
                        'units': 'm',
                        'axis': 'Y',
                    }),
                    'x': ('x', x, {
                        'standard_name': 'projection_x_coordinate',
                        'units': 'm',
                        'axis': 'X',
                    }),
                }

                arr = xr.DataArray(name=name, data=values, dims=dims, coords=coords, attrs=attrs)
                variables.append(arr)

        return variables

    def repack(self, variables):
        '''Packs a list of `DataArray` features into a `Dataset`.

        Variables of the same name are concatenated along the z- and time-axes.
        If a feature has the attribute `layer_type`, then its value becomes the
        new name of the z-axis. This allows features with different kinds of
        z-axes to live in the same dataset.

        Args:
            variables (list):
                The features to recombine.

        Returns:
            The reconstructed `Dataset`.
        '''
        def key(v):
            name = v.name
            layer_type = v.attrs['layer_type']
            forecast = v.forecast.data[0]
            z = v[layer_type].data[0]
            return (name, layer_type, forecast, z)

        logger.info('Sorting variables')
        variables = sorted(variables, key=key)

        logger.info('Reconstructing the z dimensions')
        tmp = []
        for k, g in groupby(variables, lambda v: key(v)[:3]):
            dim = k[1]
            v = xr.concat(g, dim=dim)
            tmp.append(v)
        variables = tmp

        logger.info('Reconstructing the forecast dimension')
        tmp = []
        for k, g in groupby(variables, lambda v: key(v)[:2]):
            v = xr.concat(g, dim='forecast')
            tmp.append(v)
        variables = tmp

        logger.info('Collecting the dataset')
        dataset = xr.merge(variables, join='inner')
        return dataset

    def latlon_subset(self, center=(32.8, -83.6), apo=50):
        '''Build a slice to subset NAM data around some center.

        Defaults to a square centered on Macon, GA that encompases all of
        Georgia, Alabama, and South Carolina.

        Example:
            Subset projected data given the lats and lons:
            ```
            data = ...
            subset = loader.latlon_subset()
            data[subset]
            ```

        Args:
            center (float, float):
                The center of the subset as a `(latitude, longitude)` pair.
            apo (int):
                The apothem of the subset in grid units.
                I.e. the number of cells from the center to the edge.

        Returns (slice, slice):
            A pair of slices characterizing the subset.
        '''
        # Get lats and lons from the first variable of the first grib.
        paths = tuple(self.local_gribs)
        first_file = str(paths[0])
        grbs = pygrib.open(first_file)
        g = grbs[1]  # indices start at 1
        lats, lons = g.latlons()
        return latlon_subset(lats, lons, center=center, apo=apo)

    def proj_coords(self, lats, lons):
        unproj = ccrs.PlateCarree()
        coords = PROJ.transform_points(unproj, lons, lats)
        x, y = coords[:,:,0], coords[:,:,1]
        x, y = x[0,:], y[:,0]
        x, y = np.copy(x), np.copy(y)
        return y, x

    @property
    def remote_gribs(self):
        '''An iterator over the URLs for the GRIB files in this dataset.'''
        for i in FORECAST_PERIOD:
            yield self.url_fmt.format(ref=self.ref_time, forecast=i)

    @property
    def local_gribs(self):
        '''An iterator over the paths to the local GRIB files in this dataset.'''
        for i in FORECAST_PERIOD:
            yield self.data_dir / Path(self.local_grib_fmt.format(ref=self.ref_time, forecast=i))

    @property
    def local_cdf(self):
        '''The path to the local netCDF file for this dataset.'''
        return self.data_dir / Path(self.local_cdf_fmt.format(ref=self.ref_time))


if __name__ == '__main__':
    import argparse
    import logging

    now = datetime.now(timezone.utc)

    parser = argparse.ArgumentParser(description='Download and preprocess the NAM-NMM dataset.')
    parser.add_argument('--log', type=str, help='Set the log level')
    parser.add_argument('--stop', type=lambda x: datetime.strptime(x, '%Y-%m-%dT%H00'), help='The last reference time')
    parser.add_argument('--start', type=lambda x: datetime.strptime(x, '%Y-%m-%dT%H00'), help='The first reference time')
    parser.add_argument('-n', type=int, help='The number of most recent releases to process.')
    parser.add_argument('dir', nargs='?', type=str, help='Base directory for downloads')
    args = parser.parse_args()

    log_level = args.log or 'INFO'
    logging.basicConfig(level=log_level, format='[{asctime}] {levelname}: {message}', style='{')

    data_dir = args.dir or '.'

    stop = args.stop.replace(tzinfo=timezone.utc) if args.stop else now
    delta = timedelta(hours=6)

    if args.start:
        start = args.start.replace(tzinfo=timezone.utc)
    elif args.n:
        start = stop - args.n * delta
    else:
        start = stop

    while start <= stop:
        try:
            load(start, data_dir=data_dir)
        except IOError:
            logger.warn('Could not load dataset from {}'.format(start))
        start += delta