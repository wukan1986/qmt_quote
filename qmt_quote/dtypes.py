"""
内存映射文件格式

1. 与从其他地方转存的数据格式不同
2. 与实盘计算列不同
"""
import numpy as np

DTYPE_STOCK_1t = np.dtype([
    ("stock_code", "U9"),
    ("now", np.uint64),  # 添加本地时间字段
    ("time", np.uint64),
    ("lastPrice", np.float32),  # 最新价
    ("open", np.float32),
    ("high", np.float32),
    ("low", np.float32),
    ("lastClose", np.float32),  # 昨收价
    ("amount", np.float64),
    ("volume", np.uint64),
    # ("pvolume", np.uint64),  # pvolume不维护，因为askVol/bidVol推送过来的都是手,计算vwap时要留意
    # ("stockStatus", np.int8),  # 废弃了吗？
    ("openInt", np.int8),
    ("type", np.int8),  # InstrumentType
    # ("transactionNum", np.uint32),
    # ("lastSettlementPrice", np.float32),
    # ("settlementPrice", np.float32),
    # ("pe", np.float32),
    # ("volRatio", np.float32),
    # ("speed1Min", np.float32),
    # ("speed5Min", np.float32),
    # ("avg_price", np.float32),
    ("askPrice_1", np.float32),
    ("askPrice_2", np.float32),
    ("askPrice_3", np.float32),
    ("askPrice_4", np.float32),
    ("askPrice_5", np.float32),
    ("bidPrice_1", np.float32),
    ("bidPrice_2", np.float32),
    ("bidPrice_3", np.float32),
    ("bidPrice_4", np.float32),
    ("bidPrice_5", np.float32),
    ("askVol_1", np.uint32),
    ("askVol_2", np.uint32),
    ("askVol_3", np.uint32),
    ("askVol_4", np.uint32),
    ("askVol_5", np.uint32),
    ("bidVol_1", np.uint32),
    ("bidVol_2", np.uint32),
    ("bidVol_3", np.uint32),
    ("bidVol_4", np.uint32),
    ("bidVol_5", np.uint32),
],
    align=True,
)

DTYPE_STOCK_1m = np.dtype([
    ("stock_code", "U9"),
    ("time", np.uint64),
    ("open_dt", np.uint64),
    ("close_dt", np.uint64),
    ("open", np.float32),
    ("high", np.float32),
    ("low", np.float32),
    ("close", np.float32),
    ("pre_close", np.float32),  # 昨收价。交易所发布
    ("last_close", np.float32),  # 前收价
    ("amount", np.float64),
    ("volume", np.uint64),
    ("total_amount", np.float64),
    ("total_volume", np.uint64),
    ("type", np.int8),  # InstrumentType
],
    align=True,
)

DTYPE_STOCK_1d = np.dtype([
    ("stock_code", "U9"),
    ("time", np.uint64),
    ("open_dt", np.uint64),
    ("close_dt", np.uint64),
    ("open", np.float32),
    ("high", np.float32),
    ("low", np.float32),
    ("close", np.float32),
    ("pre_close", np.float32),  # 昨收价。交易所发布
    ("amount", np.float64),
    ("volume", np.uint64),
    ("type", np.int8),  # InstrumentType
    # 盘口
    ("askPrice_1", np.float32),
    ("bidPrice_1", np.float32),
    ("askVol_1", np.uint32),
    ("bidVol_1", np.uint32),
    ("askVol_2", np.uint32),
    ("bidVol_2", np.uint32),
    # TODO 演示如何添加字段，只有历史，实盘不会对其更新
    ("circulating_cap", np.float32),  # 流通股本
    ("turnover_ratio", np.float32),  # 换手率
],
    align=True,
)

DTYPE_SIGNAL_1t = np.dtype([
    ("stock_code", "U9"),
    ("time", np.uint64),
    ("open_dt", np.uint64),
    ("close_dt", np.uint64),
    ("strategy_id", np.int16),
    ("f1", np.float32),
    ("f2", np.float32),
    ("f3", np.float32),
    ("f4", np.float32),
    ("f5", np.float32),
    ("f6", np.float32),
    ("f7", np.float32),
    ("f8", np.float32),
],
    align=True,
)
