from datetime import datetime
from typing import Tuple

import polars as pl

from examples.config import TOTAL_ASSET, BARS_PER_DAY
# from examples.factor_calc import main as factor_func_5m  # noqa
from examples.factor_calc_1d import main as factor_func_1d  # noqa
# TODO 这里简单模拟了分钟因子和日线因子
from examples.factor_calc_1m import main as factor_func_1m  # noqa
from qmt_quote.bars.labels import get_label
from qmt_quote.utils_qmt import prepare_dataframe


def main(d1d, d1m, curr_time: int, filter_last: bool) -> Tuple[pl.DataFrame, pl.DataFrame]:
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

    filter_exprs = ~pl.col('stock_code').str.starts_with('68')

    # TODO 计算因子，测试时要看所有数据，所以filter_last=False

    df1d = prepare_dataframe(d1d.tail(TOTAL_ASSET * 120), label_1d, 0, filter_exprs, pre_close='pre_close')  # 日线，要求当天K线是动态变化的
    df1d = factor_func_1d(df1d, filter_last)

    df1m = prepare_dataframe(d1m.tail(BARS_PER_DAY * 3), label_1m, label_1d, filter_exprs, pre_close='last_close')  # 1分钟线
    df1m = df1m.with_columns(
        date=pl.col('time').dt.date().cast(pl.Datetime(time_unit='ms', time_zone='Asia/Shanghai')) - pl.duration(hours=8),
    )
    df1m = df1m.join(df1d.select('stock_code', 'time', '过去5日平均每分钟成交量'), left_on=['stock_code', 'date'], right_on=['stock_code', 'time'])
    df1m = factor_func_1m(df1m, filter_last)

    return df1d, df1m
