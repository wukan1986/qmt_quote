import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from npyt import NPYT

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from examples.config import (FILE_d1m, FILE_d5m, FILE_d1d, FILE_s1t, FILE_s1d, BARS_PER_DAY, TOTAL_ASSET)
from qmt_quote.bars.labels import get_label_stock_1d
from qmt_quote.bars.signals import BarManager as BarManagerS
from qmt_quote.dtypes import DTYPE_SIGNAL_1t

from examples.strategy_base import main

# K线
d1m = NPYT(FILE_d1m).load(mmap_mode="r")
d5m = NPYT(FILE_d5m).load(mmap_mode="r")
d1d = NPYT(FILE_d1d).load(mmap_mode="r")

# TODO 策略数量，本来只用到了3个策略，但为了防止溢出，申请了4份空间
STRATEGY_COUNT = 4
# 顺序添加的信号
s1t = NPYT(FILE_s1t, dtype=DTYPE_SIGNAL_1t).save(capacity=BARS_PER_DAY * STRATEGY_COUNT).load(mmap_mode="r+")
# 日频信号
s1d = NPYT(FILE_s1d, dtype=DTYPE_SIGNAL_1t).save(capacity=TOTAL_ASSET * STRATEGY_COUNT).load(mmap_mode="r+")

# 重置信号位置
s1t.clear()
s1d.clear()

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)


def to_array_1d(df: pl.DataFrame, strategy_id: int = 0) -> np.ndarray:
    # TODO 注意：这部分的代码请根据自己实际策略进行调整
    arr = df.select(
        "stock_code",
        pl.col("time").cast(pl.UInt64),
        pl.col("open_dt").cast(pl.UInt64),
        pl.col("close_dt").cast(pl.UInt64),
        strategy_id=strategy_id,
        f1=pl.col('SIGNAL1').cast(pl.Float32),
        f2=pl.col('SIGNAL2').cast(pl.Float32),
        f3=pl.col('SIGNAL3').cast(pl.Float32),
        f4=pl.col('昨入场价延后').cast(pl.Float32),
        f5=pl.col('量比').cast(pl.Float32),
        f6=pl.lit(0, dtype=pl.Float32),
        f7=pl.col('turnover_ratio').cast(pl.Float32),
        f8=pl.lit(0, dtype=pl.Float32),
    ).select(DTYPE_SIGNAL_1t.names).to_numpy(structured=True)

    return arr


if __name__ == "__main__":
    bm_s1d = BarManagerS(s1d._a, s1d._t)

    # 由于numba class没有缓存，编译要时间
    print("=" * 60)
    # 实盘运行
    last_time = -1
    while True:
        # 调整成成分钟标签，用户可以考虑设置成10秒等更快频率。注意!!!内存映射文件要扩大几倍
        curr_time = datetime.now().timestamp() // 60 * 60
        # curr_time = datetime(2025, 7, 4, 13, 30).timestamp() // 60 * 60
        if curr_time == last_time:
            time.sleep(0.5)
            continue
        # 正好在分钟切换时才会到这一步
        last_time = curr_time
        t1 = time.perf_counter()
        df1d, df1m = main(d1d, d1m, curr_time, filter_last=True)
        t2 = time.perf_counter()
        # 测试用，观察time/open_dt/close_dt
        # print(df1m.tail(1))
        # print(df5m.tail(1))
        # print(df1d.tail(1))

        # 将3个信号增量更新到内存文件映射，只插入最新一个截面
        # s1t.append(to_array(df1m, strategy_id=1))
        # s1t.append(to_array(df5m, strategy_id=2))
        s1t.append(to_array_1d(df1d, strategy_id=3))
        #
        # 内存文件映射读取
        start, end, step = bm_s1d.extend(time.time_ns(), s1t.read(n=BARS_PER_DAY), get_label_stock_1d, 3600 * 8)
        # 只显示最新的3条
        print(end, datetime.now(), t2 - t1)
        # dd = pl.from_numpy(s1d.tail(TOTAL_ASSET)).filter(pl.col('f2')==1)
        # dd = cast_datetime(dd, col=pl.col('time', 'open_dt', 'close_dt'))
        # print(dd.to_pandas())
        dd = pd.DataFrame(s1d.tail(TOTAL_ASSET))
        dd = dd[dd['f2'] == 1]
        dd['time'] = pd.to_datetime(dd['time'], unit='ms') + pd.Timedelta(hours=8)
        dd['open_dt'] = pd.to_datetime(dd['open_dt'], unit='ms') + pd.Timedelta(hours=8)
        dd['close_dt'] = pd.to_datetime(dd['close_dt'], unit='ms') + pd.Timedelta(hours=8)
        print(dd.reset_index(drop=True))
