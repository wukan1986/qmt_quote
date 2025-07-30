"""
实盘跑时怀疑结果不正确，可以立即运行一下查看结果
"""
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl
from npyt import NPYT

from examples.strategy_base import main

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from examples.config import (FILE_d1m, FILE_d5m, FILE_d1d)

# K线
d1m = NPYT(FILE_d1m).load(mmap_mode="r")
d5m = NPYT(FILE_d5m).load(mmap_mode="r")
d1d = NPYT(FILE_d1d).load(mmap_mode="r")

if __name__ == "__main__":
    curr_time = datetime.now().timestamp() // 60 * 60
    t1 = time.perf_counter()
    df1d, df1m = main(d1d, d1m, curr_time, filter_last=False)
    t2 = time.perf_counter()

    df1d.filter(pl.col('SIGNAL1')).sort(by=['time', 'stock_code']).write_csv('1_盘前过滤.csv')
    df1d.filter(pl.col('SIGNAL2')).sort(by=['time', 'stock_code']).write_csv('2_盘后跟踪.csv')
    df1d.filter(pl.col('SIGNAL2') & ~pl.col('SIGNAL3')).sort(by=['time', 'stock_code']).write_csv('4_失败回撤.csv')
    df1m.filter(pl.col('stock_code') == '600268.SH').sort(by=['time', 'stock_code']).write_csv('3_单股分析2.csv')
