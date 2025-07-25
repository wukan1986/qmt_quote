"""
测试因子生成示例
会生成因子计算文件
"""
from expr_codegen import codegen_exec
from polars_ta.wq import *

"""
在实盘中，基本只需要每个股票最后一天的因子值，cs_rank等只需要最后一天调用就能大提速
"""


def _code_block_1():
    vwap = amount / (volume * 100)
    VWAP = vwap * factor2
    OPEN = open * factor2
    HIGH = high * factor2
    LOW = low * factor2
    CLOSE = close * factor2

    最大涨幅限制 = if_else(上海主板 | 深圳主板, 0.1, 0) + if_else(科创板 | 创业板, 0.2, 0) + if_else(北交所, 0.3, 0)
    high_limit = round_(pre_close * (1 + 最大涨幅限制), 2)
    low_limit = round_(pre_close * (1 - 最大涨幅限制), 2)

    turnover_ratio = volume / circulating_cap[1]  # 流通股本单位为万股,要换成手,*100转成%，直接省去了/10000
    过去5日平均每分钟成交量 = ts_sum(volume, 5)[1] / (240 * 5)
    量比 = volume / 过去5日平均每分钟成交量 / FROMOPEN_1(close_dt, 60)

    收盘涨停 = close >= high_limit - 0.001

    最高涨跌 = HIGH / CLOSE[1] - 1
    收盘涨跌 = CLOSE / CLOSE[1] - 1

    缩量 = ts_returns(volume) < -0.1

    # 盘前筛选。可减少计算压力 358
    SIGNAL1 = ts_shifts_v3(~收盘涨停, 0, 6, ~收盘涨停, 1, 1, 收盘涨停, 1, 3, ~收盘涨停, 1, 1)
    SIGNAL2 = ts_shifts_v1(最高涨跌 > 0, SIGNAL1)
    SIGNAL3 = ts_shifts_v1(收盘涨跌 > 0, SIGNAL1)


df = None
df = codegen_exec(df, _code_block_1, asset='stock_code', date='time', output_file='factor_calc_1d.py',
                  over_null="partition_by", filter_last=True)
