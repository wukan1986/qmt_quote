"""
Tick转分钟

对9点15到9点25之间的数据丢弃，不生成K线
"""
# import os
# os.environ['NUMBA_DISABLE_JIT'] = '1'
import os
from typing import Tuple

import numpy as np
from numba import uint64, float32, float64, uint32, typeof, int8
from numba.experimental import jitclass
from numba.typed.typeddict import Dict

from qmt_quote.dtypes import DTYPE_STOCK_1m


class Bar:
    def __init__(self, pre_close: float):
        self.close: float = pre_close
        self.pre_close: float = 0.
        self.pre_amount: float = 0.
        self.pre_volume: int = 0
        self.last_amount: float = 0.
        self.last_volume: int = 0
        self.time: int = 0  # 当前bar时间戳
        self.index: int = 0
        self.open_dt: int = 0
        self.close_dt: int = 0
        self.open: float = 0.
        self.high: float = 0.
        self.low: float = 0.
        self.type: int = 0
        self.avg_price: float = 0.
        self.askPrice_1: float = 0.
        self.bidPrice_1: float = 0.
        self.askVol_1: int = 0
        self.bidVol_1: int = 0
        self.askVol_2: int = 0
        self.bidVol_2: int = 0

    def fill(self, arr: np.ndarray, stock_code: str) -> None:
        """
        pre_amount 上一k线最后的累计金额
        pre_volume 上一k线最后的累计成交量
        last_amount 当前k线的累计金额
        last_volume 当前k线的累计成交量
        amount = last_amount - pre_amount 当前K线内产生的金额
        volume = last_volume - pre_volume 当前K线内产生的成交量

        """
        arr['stock_code'] = stock_code
        arr['time'] = self.time
        arr['open_dt'] = self.open_dt
        arr['close_dt'] = self.close_dt
        arr['open'] = self.open
        arr['high'] = self.high
        arr['low'] = self.low
        arr['close'] = self.close
        arr['preClose'] = self.pre_close
        arr['amount'] = self.last_amount - self.pre_amount
        arr['volume'] = self.last_volume - self.pre_volume
        arr['type'] = self.type
        arr['avg_price'] = self.avg_price

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

            self.pre_close = self.close
            self.pre_amount = self.last_amount
            self.pre_volume = self.last_volume
            self.open = tick['lastPrice']
            self.high = tick['lastPrice']
            self.low = tick['lastPrice']
        else:
            is_new = False
            self.high = np.maximum(tick['lastPrice'], self.high)
            self.low = np.minimum(tick['lastPrice'], self.low)

        self.close_dt = tick['time']
        self.close = tick['lastPrice']
        self.last_amount = tick['amount']
        self.last_volume = tick['volume']
        # TODO 不同类型可能不一样，先标记一下
        if tick['volume'] > 0:
            self.avg_price = tick['amount'] / tick['volume'] / 100
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
        ('pre_amount', float64),
        ('pre_volume', uint64),
        ('last_amount', float64),
        ('last_volume', uint64),
        ('type', int8),
        ('avg_price', float32),
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
        tmp['600000.SH'] = Bar(0.0)
        tmp.clear()
        self.bars = tmp

        self.arr1: np.ndarray = arr1
        self.arr2: np.ndarray = arr2
        self.index: int = int(self.arr2[1])

    def reset(self):
        self.bars.clear()
        self.index = 0
        self.arr2[1] = 0

    def extend(self, ticks: np.ndarray, get_label, get_label_arg1: float) -> Tuple[int, int, int]:
        """来ticks数据，更新bar数据

        tick不能重复，使用for_next()来获取

        """
        last_index = self.index
        for t in ticks:
            if t['open'] == 0:
                # 出现部分股票9点25过几秒open价还是0的情况
                continue
            # TODO 时间戳请选用特别的格式
            time = get_label(t['time'] // 1000, get_label_arg1) * 1000
            if time == 0:
                continue
            stock_code = str(t['stock_code'])
            # if stock_code != '301068.SZ':  # TODO test
            #     continue
            not_in = stock_code not in self.bars
            if not_in:
                self.bars[stock_code] = Bar(t['lastClose'])

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
    tmp1['600000.SH'] = Bar(0.0)
    tmp1.clear()

    idx_type = typeof(np.empty(4, dtype=np.uint64))
    bar_type = typeof(np.empty(1, dtype=DTYPE_STOCK_1m))
    spec = [
        ('bars', typeof(tmp1)),
        ('index', uint64),
        ('arr1', bar_type),
        ('arr2', idx_type),
    ]
    BarManager = jitclass(spec)(BarManager)
