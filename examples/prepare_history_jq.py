"""
从聚宽导入数据

此脚本将从ddump下载处理过后的数据处理成指定格式

"""
import pathlib
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from examples.config import HISTORY_STOCK_1d, HISTORY_STOCK_1m, HISTORY_STOCK_5m
from qmt_quote.bars.agg import convert_1m_to_5m


def adjust(df):
    df = df.with_columns(
        pl.col('stock_code').str.replace(r'\.XSHE$', '.SZ').str.replace(r'\.XSHG$', '.SH'),
        pl.col('volume') // 100,  # 将股换成手丢失了精度。因qmt的买卖一量都是手，为统一用的手
        pl.col('time').cast(pl.Datetime(time_unit='ms', time_zone='Asia/Shanghai')) - pl.duration(hours=8),  # 调整成与qmt一致的时间
    )
    return df


def save_1d(path, start_time, end_time):
    period = '1d'
    print(start_time, end_time, period)
    df = pl.read_parquet(path, columns=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'money', 'pre_close', 'paused',
                                        # TODO 按策略需求添加的字段
                                        'circulating_cap', 'turnover_ratio'])
    df = df.filter(pl.col('time') >= start_time, pl.col('time') <= end_time)
    df = adjust(df.rename({'paused': 'suspendFlag', 'code': 'stock_code', 'money': 'amount'}))
    print('沪深A股_1d===========')
    print(df.select(min_time=pl.min('time'), max_time=pl.max('time'), count=pl.count('time')))
    print(df.select(date=pl.col('time').dt.date()).group_by(by='date').agg(date=pl.last('date'), count=pl.count('date')).sort('date'))
    df.write_parquet(HISTORY_STOCK_1d)


def path_groupby_date(input_path: pathlib.Path) -> pd.DataFrame:
    """将文件名中的时间提取出来"""
    files = list(pathlib.Path(input_path).glob(f'*'))

    # 提取文件名中的时间
    df = pd.DataFrame([f.name.split('.')[0].split("__") for f in files], columns=['start', 'end'])
    df['path'] = files
    df['key1'] = pd.to_datetime(df['start'])
    df['key2'] = df['key1']
    df.index = df['key1'].copy()
    df.index.name = 'date'  # 防止无法groupby
    return df


def save_1m(path, start_time, end_time):
    period = '1m'
    print(start_time, end_time, period)

    files = path_groupby_date(path)
    files.index = pd.to_datetime(files.index.date)
    files = files[start_time:end_time]
    dfs = []
    for index, data in files.iterrows():
        dfs.append(pl.read_parquet(data['path'], columns=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'money', 'paused']))
    df: pl.DataFrame = pl.concat(dfs)
    del dfs
    df = df.filter(pl.col('time') >= start_time, pl.col('time') <= end_time)
    df = adjust(df.rename({'paused': 'suspendFlag', 'code': 'stock_code', 'money': 'amount'}))
    df = df.with_columns(
        date=pl.col('time').dt.date().cast(pl.Datetime(time_unit='ms', time_zone='Asia/Shanghai')) - pl.duration(hours=8),
    )
    # 计算总量
    df = df.with_columns(
        total_volume=pl.col('volume').cum_sum().over('stock_code', 'date', order_by='time'),
        total_amount=pl.col('amount').cum_sum().over('stock_code', 'date', order_by='time'),
    )

    df_1d = pl.read_parquet(HISTORY_STOCK_1d, columns=['time', 'stock_code', 'pre_close'])
    # 注意：pre_close是昨收，不是前收
    df = df.join(df_1d, left_on=['stock_code', 'date'], right_on=['stock_code', 'time'])
    del df_1d
    # 取前收盘价
    df = df.with_columns(
        last_close=pl.col('close').shift(1, fill_value=pl.first('pre_close')).over('stock_code', order_by='time'),
    )

    print('沪深A股_1m===========')
    print(df.select(min_time=pl.min('time'), max_time=pl.max('time'), count=pl.count('time')))
    print(df.select(date=pl.col('time').dt.date()).group_by(by='date').agg(date=pl.last('date'), count=pl.count('date')).sort('date'))
    df.write_parquet(HISTORY_STOCK_1m)


def save_5m():
    period = '5m'
    df = pl.read_parquet(HISTORY_STOCK_1m)  # .filter(pl.col('stock_code') == '000001.SZ')
    df = convert_1m_to_5m(df, period, closed="right", label="right")
    print('沪深A股_5m===========')
    print(df.select(min_time=pl.min('time'), max_time=pl.max('time'), count=pl.count('time')))
    print(df.select(date=pl.col('time').dt.date()).group_by(by='date').agg(date=pl.last('date'), count=pl.count('date')).sort('date'))
    df.write_parquet(HISTORY_STOCK_5m)


if __name__ == "__main__":
    print('请先通过jq下载数据')
    # 下午3点半后才能下载当天的数据
    end_time = datetime.now() - timedelta(hours=15, minutes=30)

    # ==========
    start_time = datetime.now() - timedelta(days=180)
    save_1d(r'F:\preprocessing\data1.parquet', start_time.date(), end_time.date())

    start_time = datetime.now() - timedelta(days=15)
    save_1m(r'D:\data\jqresearch\get_price_stock_minute', start_time.date(), end_time.date() + timedelta(days=1))
    save_5m()
