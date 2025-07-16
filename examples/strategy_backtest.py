"""
实盘跑时怀疑结果不正确，可以立即运行一下查看结果
"""
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl
from npyt import NPYT

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from examples.config import (FILE_d1d, TOTAL_ASSET)
from qmt_quote.bars.labels import get_label
from qmt_quote.utils_qmt import last_factor

# TODO 这里简单模拟了分钟因子和日线因子
from examples.factor_calc import main as factor_func_1m  # noqa
from examples.factor_calc import main as factor_func_5m  # noqa
from examples.factor_calc import main as factor_func_1d  # noqa

# K线
d1d = NPYT(FILE_d1d).load(mmap_mode="r")

TAIL_N = 120000


def main(curr_time: int) -> None:
    """
    时间正好由10:23切换到10:24,这时curr_time标记的是10:24
    10:24 bar一直在慢慢更新，10:23 bar已经固定
    分钟线建议取10:23标签，但日线建议全部
    """
    # 日线, 东八区处理
    label_1d = get_label(curr_time, 86400, tz=3600 * 8) - 0  # -0表示变化的K线，-86400，表示昨天日线

    print(datetime.fromtimestamp(curr_time))
    print(datetime.fromtimestamp(label_1d))
    # 秒转毫秒，因为qmt的时间戳是毫秒
    label_1d *= 1000

    t1 = time.perf_counter()

    filter_exprs = ~pl.col('stock_code').str.starts_with('68')

    # TODO 计算因子
    # df1m = last_factor(d1m.tail(TAIL_N), factor_func_1m, label_1m, filter_exprs)  # 1分钟线
    # df5m = last_factor(d5m.tail(TAIL_N), factor_func_5m, label_5m, filter_exprs)  # 5分钟线
    df1d = last_factor(d1d.tail(TOTAL_ASSET * 120), factor_func_1d, False, label_1d, filter_exprs)  # 日线，要求当天K线是动态变化的
    t2 = time.perf_counter()

    x1 = df1d.filter(pl.col('SIGNAL1'))
    x2 = df1d.filter(pl.col('SIGNAL2'))
    print(x1)
    print(x2)
    x1.sort(by=['time', 'stock_code']).write_csv('1_盘前过滤.csv')
    x2.sort(by=['time', 'stock_code']).write_csv('2_盘后跟踪.csv')
    df1d.filter(pl.col('stock_code') == '600192.SH').sort(by=['time', 'stock_code']).write_csv('3_单股分析2.csv')


if __name__ == "__main__":
    curr_time = datetime.now().timestamp() // 60 * 60
    main(curr_time)
