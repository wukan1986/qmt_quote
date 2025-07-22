"""
数据转换Tick数据转各种数据
1. Tick转1分钟
2. Tick转1天
3. Tick转5分钟

1. 用户可以定制此代码
2. 可盘中开启，会重新转换
3. 本项目一定要开启，因为实盘策略利用它生成的日线数据下单

原理：
申请足够大的内存映射文件，将prepaer_history.py准备好的历史数据插入到内存映射文件中
然后实时接收到的tick数据转换写入到同一个内存映射文件

历史可以是两种渠道
1. prepaer_history.py提前准备的从历史数据文件
2. subscribe_tick.py昨天前天收到的历史tick数据

1. 使用时一定要防止出现数据出现重叠区域
2. prepaer_history.py准备的数据有缺失时，可以用subscribe_tick.py的数据代替

"""
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl
from loguru import logger
from npyt import NPYT
from tqdm import tqdm

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from examples.config import (FILE_d1d, TOTAL_1d, FILE_d1m, TOTAL_1m, TOTAL_5m, FILE_d5m, FILE_d1t, TICKS_PER_MINUTE,
                             BARS_PER_DAY, TOTAL_ASSET)
from qmt_quote.bars.labels import get_label_stock_5m, get_label_stock_1m, get_label_stock_1d
from qmt_quote.bars.tick_day import BarManager as BarManagerD
from qmt_quote.bars.tick_minute import BarManager as BarManagerM
from qmt_quote.dtypes import DTYPE_STOCK_1m, DTYPE_STOCK_1t, DTYPE_STOCK_1d
from qmt_quote.enums import InstrumentType
from qmt_quote.utils_qmt import load_history_data  # noqa

# 分种级别数据，将历史TICK和实盘TICK拼接成一个实盘分钟级别数据
d1m = NPYT(FILE_d1m, dtype=DTYPE_STOCK_1m).save(capacity=TOTAL_1m).load(mmap_mode="r+")
d5m = NPYT(FILE_d5m, dtype=DTYPE_STOCK_1m).save(capacity=TOTAL_5m).load(mmap_mode="r+")
d1d = NPYT(FILE_d1d, dtype=DTYPE_STOCK_1d).save(capacity=TOTAL_1d).load(mmap_mode="r+")


def prepare_mmap(end_date: pl.datetime):
    """将历史数据写入到内存文件映射，方便实盘时直接取数据"""
    from examples.config import HISTORY_STOCK_1d, HISTORY_STOCK_1m, HISTORY_STOCK_5m  # noqa

    his_stk_1m = load_history_data(HISTORY_STOCK_1m, type=InstrumentType.Stock)
    his_stk_5m = load_history_data(HISTORY_STOCK_5m, type=InstrumentType.Stock)
    # TODO circulating_cap是额外添加的字段
    his_stk_1d = load_history_data(HISTORY_STOCK_1d, type=InstrumentType.Stock)

    # 注意当天的数据没有写入到NPYT文件
    his_stk_1m = his_stk_1m.filter(pl.col('time') < end_date)
    his_stk_5m = his_stk_5m.filter(pl.col('time') < end_date)
    his_stk_1d = his_stk_1d.filter(pl.col('time') < end_date)
    print("!!! 历史数据截断时间 <", pl.select(end_date)[0, 0])

    # TODO 调整加载的历史数据量，注意有双休和节假日
    his_stk_1m = his_stk_1m.sort('time', 'stock_code').tail(BARS_PER_DAY * 3)
    his_stk_5m = his_stk_5m.sort('time', 'stock_code').tail(BARS_PER_DAY // 5 * 5)
    his_stk_1d = his_stk_1d.sort('time', 'stock_code').tail(TOTAL_ASSET * 180)

    print("=" * 60)
    print(his_stk_1m.select(min_time=pl.min('time'), max_time=pl.max('time'), count=pl.count('time')))
    print(his_stk_5m.select(min_time=pl.min('time'), max_time=pl.max('time'), count=pl.count('time')))
    print(his_stk_1d.select(min_time=pl.min('time'), max_time=pl.max('time'), count=pl.count('time')))

    # 将历史数据添加到内存文件映射，免去计算时拼接的过程
    d1m.clear().append(his_stk_1m.select(DTYPE_STOCK_1m.names).to_numpy(structured=True))
    d5m.clear().append(his_stk_5m.select(DTYPE_STOCK_1m.names).to_numpy(structured=True))
    d1d.clear().append(his_stk_1d.select(DTYPE_STOCK_1d.names).to_numpy(structured=True))

    logger.info("d1m:{:.4f}, d5m:{:.4f}, d1d:{:.4f}, 注意：0表示空间不够没有插入，0.8以上通常空间不够当天使用，会崩溃",
                d1m.end() / d1m.capacity(),
                d5m.end() / d5m.capacity(),
                d1d.end() / d1d.capacity(),
                )


def do(file, is_live=False):
    """

    Parameters
    ----------
    file : str
        原始tick数据文件
    is_live
        是否是实盘，实盘会等待数据，历史会推出

    """
    # 文件不存在，直接返回
    if not os.path.exists(file):
        logger.error("数据文件 {} 不存在，直接返回", file)
        return

    d1t = NPYT(file, dtype=DTYPE_STOCK_1t).load(mmap_mode="r")

    # 接着昨天数据生成新K线
    bm_d1m = BarManagerM(d1m._a, d1m._t)
    bm_d5m = BarManagerM(d5m._a, d5m._t)
    bm_d1d = BarManagerD(d1d._a, d1d._t)

    bar_format = "{desc}: {percentage:5.2f}%|{bar}{r_bar}"
    # 昨天的行情数据放一起
    pbar = tqdm(total=d1t.capacity(), desc="股票+指数", initial=0, bar_format=bar_format, ncols=100)
    while True:
        # tick数据顺序调用，每一条不会重复使用
        a1t = d1t.read(n=TICKS_PER_MINUTE, prefetch=0)
        if len(a1t) == 0:
            # 没有新数据要更新，等等
            if is_live:
                time.sleep(0.5)
                continue
            else:
                break

        now = datetime.now().timestamp()
        t = a1t[-1]['now'] / 1000
        pbar.update(a1t.size)
        # 这里要refresh，否则看起来行情延时很大
        pbar.set_description(f"延时 {now - t:8.3f}s", refresh=True)

        time_ns = time.time_ns()
        bm_d1d.extend(time_ns, a1t, get_label_stock_1d, 3600 * 8)
        bm_d5m.extend(time_ns, a1t, get_label_stock_5m, 3600 * 8)
        bm_d1m.extend(time_ns, a1t, get_label_stock_1m, 3600 * 8)


if __name__ == "__main__":
    print()
    print(f"Tick数据转分钟数据。可盘中多次开启关闭。CTRL+C退出")
    print()
    print("=" * 60)

    # TODO 指定日期之前的数据从parquet中加载写入到内存文件映射
    # end_date = pl.datetime(2025, 5, 22, time_unit='ms', time_zone='Asia/Shanghai')
    end_date = pl.lit(datetime.now().date(), dtype=pl.Datetime(time_unit='ms', time_zone='Asia/Shanghai'))
    # TODO 使用今天之前的数据做历史。如果昨天的数据有问题，可以取更早一天，注意节假日
    prepare_mmap(end_date=end_date - pl.duration(days=0))

    # TODO 从指定文件加载tick数据，转换成日线和分钟
    FILE_d1t_list = [
        # 历史tick数据。注意不要与prepare_mmap的数据重叠
        # r"F:\backup\20250521\d1t.npy",
        # 当日实时tick数据
        FILE_d1t,
    ]

    print("=" * 60)
    for i, file in enumerate(FILE_d1t_list):
        do(file, is_live=i == len(FILE_d1t_list) - 1)
