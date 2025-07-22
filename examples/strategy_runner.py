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
from qmt_quote.bars.labels import get_label_stock_1d, get_label, get_traded_minutes__0900_1130__1300_1500
from qmt_quote.bars.signals import BarManager as BarManagerS
from qmt_quote.dtypes import DTYPE_SIGNAL_1t
from qmt_quote.utils_qmt import prepare_dataframe

# TODO 这里简单模拟了分钟因子和日线因子
from examples.factor_calc_1m import main as factor_func_1m  # noqa
# from examples.factor_calc import main as factor_func_5m  # noqa
from examples.factor_calc_1d import main as factor_func_1d  # noqa

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

# TODO 根据策略，在单股票上至少需要的窗口长度+1，然后乘股票数，再多留一些余量
# 窗口长度为何要+1，因为最新的K线还在变化中，为了防止信号闪烁，用户在计算前可能会剔除最后一根K线
TAIL_N = 120000


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
        f6=pl.col('turnover_ratio').cast(pl.Float32),
        f7=pl.lit(0, dtype=pl.Float32),
        f8=pl.lit(0, dtype=pl.Float32),
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

    t1 = time.perf_counter()

    filter_exprs = ~pl.col('stock_code').str.starts_with('68')
    traded_minutes = get_traded_minutes__0900_1130__1300_1500(curr_time, tz=3600 * 8)
    print(traded_minutes)

    # TODO 计算因子。一定注意filter_last=True参数，否则s1d由于空间不足报错
    filter_last = True
    df1d = prepare_dataframe(d1d.tail(TOTAL_ASSET * 120), label_1d, 0, filter_exprs, pre_close='pre_close')  # 日线，要求当天K线是动态变化的
    df1d = factor_func_1d(df1d, filter_last)
    df1d = df1d.with_columns(
        量比=pl.col('volume') / pl.col('过去5日平均每分钟成交量') / traded_minutes,
    )
    df1m = prepare_dataframe(d1m.tail(BARS_PER_DAY * 3), label_1m, 0, filter_exprs, pre_close='last_close')  # 1分钟线
    df1m = factor_func_1m(df1m, filter_last)
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
    time_ns = time.time_ns()
    start, end, step = bm_s1d.extend(time_ns, s1t.read(n=BARS_PER_DAY), get_label_stock_1d, 3600 * 8)
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
