# generate data visualizations

import matplotlib.pyplot as plt
from matplotlib import rc as matplotlib_settings
import numpy as np
import pandas as pd
import pathlib
from scipy.stats import linregress
import sqlite3
import xarray as xr

from apollo.datasets.solar import SolarDataset
import apollo.storage


def _irr_vs_hour(start, stop, output):
    """ Plot mean irradiance vs hour-of-day on the given set of axes"""
    data_dir = apollo.storage.get('GA-POWER')
    path = data_dir / 'solar_farm.sqlite'
    connection = sqlite3.connect(str(path))

    start, stop = pd.Timestamp(start), pd.Timestamp(stop)
    # convert start and stop timestamps to unix epoch in seconds
    unix_start = start.value // 10 ** 9
    unix_stop = stop.value / 10 ** 9

    # we convert from utc to est hour
    query = f' SELECT (HOUR + 20)%24 + 1 as hour,' \
            f' AVG(UGAAPOA1IRR) as Array_A,' \
            f' AVG(UGABPOA1IRR) as Array_B,' \
            f' AVG(UGAEPOA1IRR) as Array_E' \
            f' FROM IRRADIANCE WHERE timestamp' \
            f' BETWEEN {unix_start} AND {unix_stop}' \
            f' GROUP BY HOUR' \
            f' ORDER BY HOUR;'

    # lod data into df
    df = pd.read_sql_query(sql=query, con=connection, index_col='hour')
    df = df.dropna()

    fig, axes = plt.subplots()
    opacity = 0.6
    axes.set_title('Mean Irradiance vs Time-of-Day')
    line1, = axes.plot(df.index, df['Array_A'],
                       color='r', alpha=opacity, label='Array A')
    line2, = axes.plot(df.index, df['Array_B'],
                       color='g', alpha=opacity, label='Array B')
    line3, = axes.plot(df.index, df['Array_E'],
                       color='b', alpha=opacity, label='Array E')
    axes.set_xticks(range(1, 25))
    axes.set_xlabel('Hour (EST)')
    axes.set_ylabel('Mean Irradiance (watts / m$^2$)')
    axes.legend()

    # set size and font sizes
    fig.set_size_inches(10, 6)
    axes.title.set_fontsize(16)
    axes.xaxis.label.set_fontsize(14)
    axes.yaxis.label.set_fontsize(14)
    plt.savefig(str(output / 'irr_vs_hour.png'))
    plt.close(fig)


def _irr_vs_month(start, stop, output):
    """ Plot mean irradiance vs month on the given set of axes """
    data_dir = apollo.storage.get('GA-POWER')
    path = data_dir / 'solar_farm.sqlite'
    connection = sqlite3.connect(str(path))

    start, stop = pd.Timestamp(start), pd.Timestamp(stop)

    # convert start and stop timestamps to unix epoch in seconds
    unix_start = start.value // 10 ** 9
    unix_stop = stop.value / 10 ** 9

    # we convert from utc to est hour
    query = f' SELECT MONTH as month, ' \
            f' AVG(UGAAPOA1IRR) as Array_A,' \
            f' AVG(UGABPOA1IRR) as Array_B,' \
            f' AVG(UGAEPOA1IRR) as Array_E' \
            f' FROM IRRADIANCE WHERE timestamp' \
            f' BETWEEN {unix_start} AND {unix_stop}' \
            f' GROUP BY month' \
            f' ORDER BY month;'

    # lod data into df
    df = pd.read_sql_query(sql=query, con=connection, index_col='month')
    df = df.dropna()

    bar_width = 0.25
    opacity = 0.6

    fig, axes = plt.subplots()
    axes.set_title('Mean Irradiance per Month')
    rects1 = axes.bar(df.index - bar_width, df['Array_A'], width=bar_width,
                      color='r', alpha=opacity, label='Array A')
    rects2 = axes.bar(df.index, df['Array_B'], width=bar_width,
                      color='g', alpha=opacity, label='Array B')
    rects3 = axes.bar(df.index + bar_width, df['Array_E'], width=bar_width,
                      color='b', alpha=opacity, label='Array E')
    axes.set_xticks(range(1, 13))
    axes.set_xticklabels((
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ))
    axes.set_xlabel('Month')
    axes.set_ylabel('Mean Irradiance (watts / m$^2$)')
    axes.legend()

    # set size and font sizes
    fig.set_size_inches(10, 6)
    axes.title.set_fontsize(16)
    axes.xaxis.label.set_fontsize(14)
    axes.yaxis.label.set_fontsize(14)
    plt.savefig(str(output / 'irr_vs_month.png'))
    plt.close(fig)


def _irr_correlations(start, stop, output):
    start, stop = pd.Timestamp(start), pd.Timestamp(stop)

    # select targets
    data_dir = apollo.storage.get('GA-POWER')
    path = data_dir / 'solar_farm.sqlite'
    connection = sqlite3.connect(str(path))

    # convert start and stop timestamps to unix epoch in seconds
    unix_start = start.value // 10 ** 9
    unix_stop = stop.value / 10 ** 9

    targets_query = f' SELECT TIMESTAMP as reftime, ' \
                    f' AVG(UGAAPOA1IRR) as Array_A,' \
                    f' AVG(UGABPOA1IRR) as Array_B,' \
                    f' AVG(UGAEPOA1IRR) as Array_E ' \
                    f' FROM IRRADIANCE WHERE reftime' \
                    f' BETWEEN {unix_start} AND {unix_stop} ' \
                    f' AND hour=18 ' \
                    f' GROUP BY hour, day, month, year;'
    # load data and aggregate by hour
    targets_ds = pd.read_sql_query(sql=targets_query, con=connection,
                                   index_col='reftime', parse_dates=['reftime'])
    targets_ds = targets_ds.dropna()
    # timestamp index is unix epoch (in seconds)
    targets_ds.index = pd.to_datetime(targets_ds.index, unit='s')
    targets_ds.index.name = 'reftime'
    targets_ds = targets_ds.to_xarray()

    cloud_cover_var = 'TCC_EATM'
    air_temp_var = 'TMP_SFC'
    downward_flux_var = 'DLWRF_SFC'
    cloud_cover_ds = SolarDataset(
        start=start, stop=stop, target=None, forecast=0,
        feature_subset=(cloud_cover_var, air_temp_var, downward_flux_var),
        temporal_features=False, geo_shape=(1,1), standardize=False,
        target_hours=0).xrds

    # we have only selected data along the reftime coordinate
    cloud_cover_ds = cloud_cover_ds.drop(
        labels=('x', 'y', 'lat', 'lon', 'forecast', 'z_EATM')).squeeze()

    dataset = xr.merge([targets_ds, cloud_cover_ds], join='inner')

    array_a = dataset['Array_A'].values
    array_b = dataset['Array_B'].values
    array_e = dataset['Array_E'].values

    # plot irradiance vs cloud cover
    fig, axes = plt.subplots()
    x = dataset[cloud_cover_var].values
    axes.scatter(x, array_a, color='r', alpha=0.5, label='Array A')
    axes.scatter(x, array_b, color='g', alpha=0.5, label='Array B')
    axes.scatter(x, array_e, color='b', alpha=0.5, label='Array E')

    # plot trend lines
    colors = ('r', 'g', 'b')
    for idx, values in enumerate([array_a, array_b, array_e]):
        slope, intercept, *rest = linregress(x, values)
        min_x, max_x = min(x), max(x)
        line_endpoints = min_x*slope + intercept, max_x*slope + intercept
        axes.plot((min_x, max_x), line_endpoints,
                  f'--{colors[idx]}', label='Trend Line')

    axes.set_title('Irradiance at 2PM EST vs. Total Cloud Cover')
    axes.set_xlabel('Cloud Cover (%)')
    axes.set_ylabel('Irradiance (watts / m$^2$)')
    axes.set_xticks(np.arange(0, 110, 10))
    axes.legend()

    # set size and font sizes
    fig.set_size_inches(10, 6)
    axes.title.set_fontsize(16)
    axes.xaxis.label.set_fontsize(14)
    axes.yaxis.label.set_fontsize(14)
    plt.savefig(str(output / 'irr_vs_clouds.png'))

    # plot irradiance vs air temperature
    plt.cla()
    x = dataset[air_temp_var].values
    axes.scatter(x, array_a, color='r', label='Array A')
    axes.scatter(x, array_b, color='g', label='Array B')
    axes.scatter(x, array_e, color='b', label='Array E')

    # plot trend lines
    colors = ('r', 'g', 'b')
    for idx, values in enumerate([array_a, array_b, array_e]):
        slope, intercept, *rest = linregress(x, values)
        min_x, max_x = min(x), max(x)
        line_endpoints = min_x*slope + intercept, max_x*slope + intercept
        axes.plot((min_x, max_x), line_endpoints,
                  f'--{colors[idx]}', label='Trend Line')

    axes.set_title('Irradiance at 2PM EST vs. Surface Air Temperature')
    axes.set_xlabel('Air Tempurature (Kelvin)')
    axes.set_ylabel('Irradiance (watts / m$^2$)')
    axes.legend()

    # set size and font sizes
    fig.set_size_inches(10, 6)
    axes.title.set_fontsize(16)
    axes.xaxis.label.set_fontsize(14)
    axes.yaxis.label.set_fontsize(14)
    plt.savefig(str(output / 'irr_vs_temp.png'))

    # plot irradiance vs air temperature
    plt.cla()
    x = dataset[downward_flux_var].values
    axes.scatter(x, array_a, color='r', label='Array A')
    axes.scatter(x, array_b, color='g', label='Array B')
    axes.scatter(x, array_e, color='b', label='Array E')

    # plot trend lines
    colors = ('r', 'g', 'b')
    for idx, values in enumerate([array_a, array_b, array_e]):
        slope, intercept, *rest = linregress(x, values)
        min_x, max_x = min(x), max(x)
        line_endpoints = min_x*slope + intercept, max_x*slope + intercept
        axes.plot((min_x, max_x), line_endpoints,
                  f'--{colors[idx]}', label='Trend Line')

    axes.set_title('Irradiance at 2PM EST vs. Downwelling Longwave Flux')
    axes.set_xlabel('Downwelling Longwave Flux (watts / m$^2$)')
    axes.set_ylabel('Irradiance (watts / m$^2$)')
    axes.legend()

    # set size and font sizes
    fig.set_size_inches(10, 6)
    axes.title.set_fontsize(16)
    axes.xaxis.label.set_fontsize(14)
    axes.yaxis.label.set_fontsize(14)
    plt.savefig(str(output / 'irr_vs_flux.png'))
    plt.close(fig)


def run(output='./output/figures', first='2017-01-01', last='2018-12-31'):
    outpath = pathlib.Path(output).resolve()
    outpath.mkdir(parents=True, exist_ok=True)

    matplotlib_settings('font', family='normal', size=12)

    _irr_vs_hour(start=first, stop=last, output=outpath)
    _irr_vs_month(start=first, stop=last, output=outpath)
    _irr_correlations(start=first, stop=last, output=outpath)


if __name__ == '__main__':
    run()
