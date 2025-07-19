"""
Tick转日线

Tick中有日线全部数据，理论上只要取最后一段就可以了，
但还要用于日K线的实时生成和下单前取快照，所以是用的全量数据实时生成

"""
# import os
#
# os.environ['NUMBA_DISABLE_JIT'] = '1'
import os
from typing import Tuple

import numpy as np
from numba import uint64, float32, float64, uint32, typeof, int8
from numba.experimental import jitclass
from numba.typed.typeddict import Dict

from qmt_quote.dtypes import DTYPE_STOCK_1d


class Bar:
    def __init__(self):
        self.close: float = 0.
        self.pre_close: float = 0.
        self.amount: float = 0.
        self.volume: int = 0
        self.time: int = 0  # 当前bar时间戳
        self.index: int = 0
        self.open_dt: int = 0
        self.close_dt: int = 0
        self.open: float = 0.
        self.high: float = 0.
        self.low: float = 0.
        self.type: int = 0

        self.askPrice_1: float = 0.
        self.bidPrice_1: float = 0.
        self.askVol_1: int = 0
        self.bidVol_1: int = 0
        self.askVol_2: int = 0
        self.bidVol_2: int = 0

    def fill(self, arr: np.ndarray, stock_code: str) -> None:
        arr['stock_code'] = stock_code
        arr['time'] = self.time
        arr['open_dt'] = self.open_dt
        arr['close_dt'] = self.close_dt
        arr['open'] = self.open
        arr['high'] = self.high
        arr['low'] = self.low
        arr['close'] = self.close
        arr['pre_close'] = self.pre_close
        arr['amount'] = self.amount
        arr['volume'] = self.volume
        arr['type'] = self.type

        arr['askPrice_1'] = self.askPrice_1
        arr['bidPrice_1'] = self.bidPrice_1
        arr['askVol_1'] = self.askVol_1
        arr['bidVol_1'] = self.bidVol_1
        arr['askVol_2'] = self.askVol_2
        arr['bidVol_2'] = self.bidVol_2

    def update(self, tick: np.ndarray, time: int) -> bool:
        """数据增量更新，同一条tick不会重复使用

        后一段会随着新数据到来而更新。前一段已经成为了历史不再变化

        """
        if self.time != time:
            self.time = time
            is_new = True
            self.open_dt = tick['time']
            self.type = tick['type']

            self.pre_close = tick['lastClose']
        else:
            is_new = False

        self.open = tick['open']  # 部分tick9点25之前0
        self.high = tick['high']
        self.low = tick['low']

        self.close_dt = tick['time']
        self.close = tick['lastPrice']
        self.amount = tick['amount']
        self.volume = tick['volume']

        self.askPrice_1 = tick['askPrice_1']
        self.bidPrice_1 = tick['bidPrice_1']
        self.askVol_1 = tick['askVol_1']
        self.bidVol_1 = tick['bidVol_1']
        self.askVol_2 = tick['askVol_2']
        self.bidVol_2 = tick['bidVol_2']

        return is_new


if os.environ.get('NUMBA_DISABLE_JIT', '0') != '1':
    spec = [
        ('index', uint64),
        ('time', uint64),
        ('open_dt', uint64),
        ('close_dt', uint64),
        ('open', float32),
        ('high', float32),
        ('low', float32),
        ('close', float32),
        ('pre_close', float32),
        ('amount', float64),
        ('volume', uint64),
        ('type', int8),

        ('askPrice_1', float32),
        ('bidPrice_1', float32),
        ('askVol_1', uint32),
        ('bidVol_1', uint32),
        ('askVol_2', uint32),
        ('bidVol_2', uint32),
    ]
    Bar = jitclass(spec)(Bar)


class BarManager:

    def __init__(self, arr1: np.ndarray, arr2: np.ndarray):
        tmp = Dict()
        tmp['600000.SH'] = Bar()
        tmp.clear()
        self.bars = tmp
        # self.bars = dict()

        self.arr1: np.ndarray = arr1
        self.arr2: np.ndarray = arr2
        self.index: int = int(self.arr2[1])

    def reset(self):
        self.bars.clear()
        self.index = 0
        self.arr2[1] = 0

    def extend(self, ticks: np.ndarray, get_label, get_label_arg1: int) -> Tuple[int, int, int]:
        """来ticks数据，更新bar数据

        tick不能重复，使用for_next()来获取

        """
        last_index = self.index
        for t in ticks:
            # TODO 时间戳请选用特别的格式
            time = get_label(t['time'] // 1000, get_label_arg1) * 1000
            stock_code = str(t['stock_code'])
            not_in = stock_code not in self.bars
            if not_in:
                self.bars[stock_code] = Bar()

            bb = self.bars[stock_code]
            if bb.update(t, time):
                bb.index = self.index
                self.index += 1
            bb.fill(self.arr1[bb.index], stock_code)
        # 记录位子
        self.arr2[1] = self.index
        return last_index, self.index, self.index - last_index


if os.environ.get('NUMBA_DISABLE_JIT', '0') != '1':
    tmp1 = Dict()
    tmp1['600000.SH'] = Bar()
    tmp1.clear()

    idx_type = typeof(np.empty(4, dtype=np.uint64))
    bar_type = typeof(np.empty(1, dtype=DTYPE_STOCK_1d))
    spec = [
        ('bars', typeof(tmp1)),
        ('index', uint64),
        ('arr1', bar_type),
        ('arr2', idx_type),
    ]
    BarManager = jitclass(spec)(BarManager)
