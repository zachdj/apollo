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
from apollo.datasets.nam import open_range
import apollo.storage
from apollo.viz import nam_map, date_heatmap_figure


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
    axes.set_ylabel('Mean Irradiance (W / m$^2$)')
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
    axes.set_ylabel('Mean Irradiance (W / m$^2$)')
    axes.legend()

    # set size and font sizes
    fig.set_size_inches(10, 6)
    axes.title.set_fontsize(16)
    axes.xaxis.label.set_fontsize(14)
    axes.yaxis.label.set_fontsize(14)
    plt.savefig(str(output / 'irr_vs_month.png'))
    plt.close(fig)


def _irr_correlations(start, stop, output,
                      data_vars=(
                              'TCC_EATM',
                              'TMP_SFC',
                              'DSWRF_SFC',
                              'DLWRF_SFC',
                              'PRES_SFC',
                              'LHTFL_SFC',
                              'VIS_SFC'),
                      data_var_labels=(
                              'Total Cloud Cover',
                              'Surface Air Temperature',
                              'Downwelling Shortwave Flux',
                              'Downwelling Longwave Flux',
                              'Air Pressure',
                              'Upward Latent Heat Flux',
                              'Visibility'),
                      data_var_units=(
                              '%',
                              'K',
                              'W m$^{-2}$',
                              'W m$^{-2}$',
                              'Pa',
                              'W m$^{-2}$',
                              'm'),
                      colors=('r', 'g', 'b'),
                      alpha=0.5):
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

    weather_var_ds = SolarDataset(
        start=start, stop=stop, target=None, target_hours=0, forecast=0,
        feature_subset=data_vars, temporal_features=False,
        geo_shape=(1,1), standardize=False).xrds

    # we have only selected data along the reftime coordinate
    weather_var_ds = weather_var_ds.drop(
        labels=('x', 'y', 'lat', 'lon', 'forecast', 'z_EATM')).squeeze()

    dataset = xr.merge([targets_ds, weather_var_ds], join='inner')

    array_a = dataset['Array_A'].values
    array_b = dataset['Array_B'].values
    array_e = dataset['Array_E'].values

    # plot figures with trend lines
    for data_var_idx, data_var in enumerate(data_vars):
        print(f'* Plotting {data_var}')
        plt.cla()
        fig, axes = plt.subplots()
        x = dataset[data_var].values
        axes.scatter(x, array_a, color=colors[0], alpha=alpha, label='Array A')
        axes.scatter(x, array_b, color=colors[1], alpha=alpha, label='Array B')
        axes.scatter(x, array_e, color=colors[2], alpha=alpha, label='Array E')

        # trend line
        for idx, values in enumerate([array_a, array_b, array_e]):
            slope, intercept, *_unused = linregress(x, values)
            min_x, max_x = min(x), max(x)
            line_endpoints = min_x*slope + intercept, max_x*slope + intercept
            axes.plot((min_x, max_x), line_endpoints,
                      f'--{colors[idx]}', label='Trend Line')

        axes.set_title(
            f'Irradiance at 2PM EST vs. {data_var_labels[data_var_idx]}')
        axes.set_xlabel(
            f'{data_var_labels[data_var_idx]} ({data_var_units[data_var_idx]})')
        axes.set_ylabel('Irradiance (W m$^{-2}$)')
        if data_var_units[data_var_idx] == '%':
            axes.set_xticks(np.arange(0, 110, 10))
        axes.legend()

        # set size and font sizes
        fig.set_size_inches(10, 6)
        axes.title.set_fontsize(16)
        axes.xaxis.label.set_fontsize(14)
        axes.yaxis.label.set_fontsize(14)
        plt.savefig(str(output / f'irr_vs_{data_var}.png'))


def data_availability(start='2017-01-01', stop='2018-12-31',
                      output='./results/figures'):
    ds = SolarDataset(start=start, stop=stop,
                      target=None, target_hours=0, forecast=0,
                      feature_subset=('DSWRF_SFC',), temporal_features=False,
                      geo_shape=(1,1), standardize=False).xrds

    ds = ds.drop(
        labels=('x', 'y', 'lat', 'lon', 'forecast', 'z_SFC')
    ).squeeze().to_dataframe()
    series = pd.Series(index=ds.index, data=np.ones(len(ds.index)).astype(np.int))
    outpath = pathlib.Path(output) / 'heatmap.png'
    fig = date_heatmap_figure(series=series, cmap='Blues', savefig=str(outpath))


def nam_heatmap(start='2017-06-01 T18:00:00',
                output='./results/figures', target_var='DSWRF_SFC'):
    ds = open_range(start=start, stop=pd.Timestamp(start) + pd.Timedelta(6, 'h'))
    fig = nam_map(xrds=ds, feature=target_var, reftime=0, forecast=0,
                  title='')
    for ax in fig.axes:
        ax.set_ylabel('Downwelling Shortwave Flux (W m$^{-2}$)')

    outpath = pathlib.Path(output) / 'nam_map.png'
    plt.savefig(str(outpath))


def run(first='2017-01-01', last='2018-12-31',
        output='./results/figures', figure_set='all'):
    outpath = pathlib.Path(output).resolve()
    outpath.mkdir(parents=True, exist_ok=True)

    matplotlib_settings('font', family='normal', size=12)

    if figure_set == 'irr' or figure_set == 'all':
        _irr_vs_hour(start=first, stop=last, output=outpath)
        _irr_vs_month(start=first, stop=last, output=outpath)
        _irr_correlations(start=first, stop=last, output=outpath)

    if figure_set == 'heatmaps' or figure_set == 'all':
        data_availability(start=first, stop=last, output=outpath)
        nam_heatmap(start=first, output=outpath)


if __name__ == '__main__':
    run()
