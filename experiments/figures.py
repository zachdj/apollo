# generate data visualizations

import matplotlib.pyplot as plt
import pandas as pd
import pathlib
import sqlite3

import apollo.storage


def _irr_vs_hour(fig, axes):
    """ Plot mean irradiance vs hour-of-day on the given set of axes"""
    data_dir = apollo.storage.get('GA-POWER')
    path = data_dir / 'solar_farm.sqlite'
    connection = sqlite3.connect(str(path))

    start, stop = pd.Timestamp('2017-01-01'), pd.Timestamp('2018-12-31')

    # convert start and stop timestamps to unix epoch in seconds
    unix_start = start.value // 10 ** 9
    unix_stop = stop.value / 10 ** 9

    # we convert from utc to est hour
    query = f'SELECT (HOUR + 20)%24 + 1 as hour, ' \
            f'AVG(UGAAPOA1IRR) as Array_A, AVG(UGABPOA1IRR) as Array_B, AVG(UGAEPOA1IRR) as Array_E '
    query += f' FROM IRRADIANCE WHERE timestamp BETWEEN {unix_start} AND {unix_stop}' \
             f' GROUP BY HOUR' \
             f' ORDER BY HOUR;'

    # lod data into df
    df = pd.read_sql_query(sql=query, con=connection, index_col='hour')
    df = df.dropna()

    opacity = 0.6

    axes.set_title('Mean Irradiance vs Time-of-Day')
    line1, = axes.plot(df.index, df['Array_A'], color='r', alpha=opacity, label='Array A')
    line2, = axes.plot(df.index, df['Array_B'], color='g', alpha=opacity, label='Array B')
    line3, = axes.plot(df.index, df['Array_E'], color='b', alpha=opacity, label='Array E')
    axes.set_xticks(range(1, 25))
    axes.set_xlabel('Hour (EST)')
    axes.set_ylabel('Mean Irradiance (watts / m$^2$)')
    axes.legend()


def _irr_vs_month(fig, axes):
    """ Plot mean irradiance vs month on the given set of axes """
    data_dir = apollo.storage.get('GA-POWER')
    path = data_dir / 'solar_farm.sqlite'
    connection = sqlite3.connect(str(path))

    start, stop = pd.Timestamp('2017-01-01'), pd.Timestamp('2018-12-31')

    # convert start and stop timestamps to unix epoch in seconds
    unix_start = start.value // 10 ** 9
    unix_stop = stop.value / 10 ** 9

    # we convert from utc to est hour
    query = f'SELECT MONTH as month, ' \
            f'AVG(UGAAPOA1IRR) as Array_A, AVG(UGABPOA1IRR) as Array_B, AVG(UGAEPOA1IRR) as Array_E '
    query += f' FROM IRRADIANCE WHERE timestamp BETWEEN {unix_start} AND {unix_stop}' \
             f' GROUP BY month' \
             f' ORDER BY month;'

    # lod data into df
    df = pd.read_sql_query(sql=query, con=connection, index_col='month')
    df = df.dropna()

    bar_width = 0.25
    opacity = 0.6

    axes.set_title('Mean Irradiance per Month')
    rects1 = axes.bar(df.index - bar_width, df['Array_A'], width=bar_width, color='r', alpha=opacity, label='Array A')
    rects2 = axes.bar(df.index, df['Array_B'], width=bar_width, color='g', alpha=opacity, label='Array B')
    rects3 = axes.bar(df.index + bar_width, df['Array_E'], width=bar_width, color='b', alpha=opacity, label='Array E')
    axes.set_xticks(range(1, 13))
    axes.set_xticklabels((
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ))
    axes.set_xlabel('Month')
    axes.set_ylabel('Mean Irradiance (watts / m$^2$)')
    axes.legend()


def run(output='./figures'):
    outpath = pathlib.Path(output).resolve()
    outpath.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots()
    _irr_vs_hour(fig, axes)
    fig.set_size_inches(10, 6)
    plt.savefig(str(outpath / 'irr_vs_hour.png'))

    fig, axes = plt.subplots()
    _irr_vs_month(fig, axes)
    fig.set_size_inches(10, 6)
    plt.savefig(str(outpath / 'irr_vs_month.png'))


if __name__ == '__main__':
    run()
