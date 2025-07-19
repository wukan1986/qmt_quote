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
    vwap = total_amount / total_volume / 100
    VWAP = vwap * factor2
    OPEN = open * factor2
    HIGH = high * factor2
    LOW = low * factor2
    CLOSE = close * factor2

    最大涨幅限制 = if_else(上海主板 | 深圳主板, 0.1, 0) + if_else(科创板 | 创业板, 0.2, 0) + if_else(北交所, 0.3, 0)
    high_limit = round_(pre_close * (1 + 最大涨幅限制), 2)
    low_limit = round_(pre_close * (1 - 最大涨幅限制), 2)


df = None
df = codegen_exec(df, _code_block_1, asset='stock_code', date='time', output_file='factor_calc_1m.py',
                  over_null="partition_by", filter_last=True)
