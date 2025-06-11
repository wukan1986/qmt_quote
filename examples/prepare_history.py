"""
使用prepaer_history.py准备数据时一定要留意数据的时间是否够。本人多次遇到QMT下不动，或提示下载完成，但数据只下了一截

普通版和极速版可以同时开启

建议交易日前一天就提前下载好，并准备好数据，然后看情况是否调整subscribe_minute.py的配置
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from loguru import logger
from xtquant import xtdata

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from examples.config import HISTORY_STOCK_1d, HISTORY_INDEX_1d, HISTORY_STOCK_1m, DATA_DIR, HISTORY_STOCK_5m
from qmt_quote.bars.agg import convert_1m_to_5m
from qmt_quote.utils_qmt import get_local_data_wrap

# 开盘前需要先更新板块数据，因为会有新股上市
xtdata.download_sector_data()

G = Exception()
G.沪深A股 = xtdata.get_stock_list_in_sector("沪深A股")
G.沪深指数 = xtdata.get_stock_list_in_sector("沪深指数")


def save_1d(start_time, end_time):
    period = '1d'
    print(start_time, end_time, period)
    df = get_local_data_wrap(G.沪深A股, period, start_time, end_time, data_dir=DATA_DIR)
    print('沪深A股_1d===========')
    print(df.select(min_time=pl.min('time'), max_time=pl.max('time'), count=pl.count('time')))
    df.write_parquet(HISTORY_STOCK_1d)

    # print(df)
    df = get_local_data_wrap(G.沪深指数, period, start_time, end_time, data_dir=DATA_DIR)
    print('沪深指数_1d===========')
    print(df.select(min_time=pl.min('time'), max_time=pl.max('time'), count=pl.count('time')))
    df.write_parquet(HISTORY_INDEX_1d)


def save_1m(start_time, end_time):
    period = '1m'
    print(start_time, end_time, period)
    df = get_local_data_wrap(G.沪深A股, period, start_time, end_time, data_dir=DATA_DIR)
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
    print('1. 请先在QMT普通版中手动下数据')
    print('2. 然后在QMT极速版中运行本脚本')
    print('3. 一定要查看数据是否下载完整')
    # 下午3点半后才能下载当天的数据
    end_time = datetime.now() - timedelta(hours=15, minutes=30)
    end_time = end_time.strftime("%Y%m%d")
    # end_time = "20250213"  # 测试用，以后要注释

    # ==========
    # logger.info('开始转存数据。请根据自己策略预留一定长度的数据')
    start_time = datetime.now() - timedelta(days=30)
    start_time = start_time.strftime("%Y%m%d")
    save_1d(start_time, end_time)

    start_time = datetime.now() - timedelta(days=15)
    start_time = start_time.strftime("%Y%m%d")
    save_1m(start_time, end_time)
    # ==========
    logger.info('1分钟转5分钟')
    save_5m()
