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

from examples.config import (FILE_d1m, FILE_d1d, FILE_d5m, FILE_s1t, FILE_s1d, BARS_PER_DAY, TOTAL_ASSET)
from qmt_quote.bars.labels import get_label_stock_1d, get_label
from qmt_quote.bars.signals import BarManager as BarManagerS
from qmt_quote.dtypes import DTYPE_SIGNAL_1t, DTYPE_SIGNAL_1m
from qmt_quote.utils_qmt import last_factor

# TODO 这里简单模拟了分钟因子和日线因子
from examples.factor_calc import main as factor_func_1m  # noqa
from examples.factor_calc import main as factor_func_5m  # noqa
from examples.factor_calc import main as factor_func_1d  # noqa

# K线
d1m = NPYT(FILE_d1m).load(mmap_mode="r")
d5m = NPYT(FILE_d5m).load(mmap_mode="r")
d1d = NPYT(FILE_d1d).load(mmap_mode="r")

# TODO 策略数量，本来只用到了3个策略，但为了防止溢出，申请了4份空间
STRATEGY_COUNT = 4
# 顺序添加的信号
s1t = NPYT(FILE_s1t, dtype=DTYPE_SIGNAL_1t).save(capacity=BARS_PER_DAY * STRATEGY_COUNT).load(mmap_mode="r+")
# 日频信号
s1d = NPYT(FILE_s1d, dtype=DTYPE_SIGNAL_1m).save(capacity=TOTAL_ASSET * STRATEGY_COUNT).load(mmap_mode="r+")

# 重置信号位置
s1t.clear()
s1d.clear()

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# TODO 根据策略，在单股票上至少需要的窗口长度+1，然后乘股票数，再多留一些余量
# 窗口长度为何要+1，因为最新的K线还在变化中，为了防止信号闪烁，用户在计算前可能会剔除最后一根K线
TAIL_N = 120000


def to_array(df: pl.DataFrame, strategy_id: int = 0) -> np.ndarray:
    arr = df.select(
        "stock_code", pl.col("time").cast(pl.UInt64),
        strategy_id=strategy_id,
        float32=pl.col('A').cast(pl.Float32),
        int32=pl.col('B').fill_null(0).cast(pl.Int32),
        boolean=pl.col('OUT').cast(pl.Boolean),
    ).select(DTYPE_SIGNAL_1t.names).to_numpy(structured=True)

    return arr


def main(curr_time: int) -> None:
    """
    时间正好由10:23切换到10:24,这时curr_time标记的是10:24
    10:24 bar一直在慢慢更新，10:23 bar已经固定
    分钟线建议取10:23标签，但日线建议全部
    """
    # 过滤时间。调整成分钟标签，是取当前更新中的K线，还是取上一根不变的K线？
    label_1m = get_label(curr_time, 60, tz=3600 * 8) - 60  # 前60秒，取的是已经不变化的K线
    label_5m = get_label(curr_time, 300, tz=3600 * 8) - 0  # -0表示变化的K线，-300前300秒固定K线
    # 日线, 东八区处理
    label_1d = get_label(curr_time, 86400, tz=3600 * 8) - 0  # -0表示变化的K线，-86400，表示昨天日线

    print(datetime.fromtimestamp(curr_time))
    print(datetime.fromtimestamp(label_1m))
    print(datetime.fromtimestamp(label_5m))
    print(datetime.fromtimestamp(label_1d))
    # 秒转毫秒，因为qmt的时间戳是毫秒
    label_1m *= 1000
    label_5m *= 1000
    label_1d *= 1000

    t1 = time.perf_counter()

    filter_exprs = ~pl.col('stock_code').str.starts_with('68')

    # TODO 计算因子
    df1m = last_factor(d1m.tail(TAIL_N), factor_func_1m, label_1m, filter_exprs, pl.col('time') >= label_1m)  # 1分钟线
    df5m = last_factor(d5m.tail(TAIL_N), factor_func_5m, label_5m, filter_exprs, pl.col('time') >= label_5m)  # 5分钟线
    df1d = last_factor(d1d.tail(TAIL_N), factor_func_1d, label_1d, filter_exprs, pl.col('time') >= label_1d)  # 日线，要求当天K线是动态变化的
    t2 = time.perf_counter()

    # if df1m.is_empty():
    #     print("没有1分钟数据，返回")
    #     return

    # 测试用，观察time/open_dt/close_dt
    print(df1m.tail(1))
    print(df5m.tail(1))
    print(df1d.tail(1))

    # 将3个信号增量更新到内存文件映射
    s1t.append(to_array(df1m, strategy_id=1))
    s1t.append(to_array(df5m, strategy_id=2))
    s1t.append(to_array(df1d, strategy_id=3))

    # 内存文件映射读取
    start, end, step = bm_s1d.extend(s1t.read(n=BARS_PER_DAY), get_label_stock_1d, 3600 * 8)
    # 只显示最新的3条
    print(end, datetime.now(), t2 - t1)
    print(s1d.tail(3))


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
        main(curr_time)

    # # TODO 测试用，记得修改日期
    # for curr_time in range(int(datetime(2025, 3, 5, 9, 29).timestamp() // 60 * 60),
    #                        int(datetime(2025, 3, 5, 15, 1).timestamp() // 60 * 60),
    #                        60):
    #     # 调整成成分钟标签，当前分钟还在更新
    #     # curr_time = datetime(2025, 2, 28, 15, 0).timestamp() // 60 * 60
    #     last_time = curr_time
    #     main(curr_time)
