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
    vwap = amount / volume
    VWAP = vwap * factor2
    OPEN = open * factor2
    HIGH = high * factor2
    LOW = low * factor2
    CLOSE = close * factor2

    最大涨幅限制 = if_else(上海主板 | 深圳主板, 0.1, 0) + if_else(科创板 | 创业板, 0.2, 0) + if_else(北交所, 0.3, 0)
    # preClose是上一K线的收盘价，所以high_limit/low_limit只在日线中才正确
    high_limit = preClose * (1 + 最大涨幅限制)
    low_limit = preClose * (1 - 最大涨幅限制)

    MA5 = ts_mean(CLOSE, 5)
    MA10 = ts_mean(CLOSE, 10)
    A = ts_returns(CLOSE, 5)
    B = cs_rank(-A, False)
    OUT = B <= 5


# df = pl.read_parquet(HISTORY_STOCK_1d)
# df = calc_factor1(df, by1='stock_code', by2='time', close='close', pre_close='preClose')
df = None
df = codegen_exec(df, _code_block_1, asset='stock_code', date='time', output_file='factor_calc.py',
                  over_null="partition_by", filter_last=True)
# print(df.tail())
