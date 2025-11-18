"""
与交易平台无关的工具函数

"""
from typing import Dict, Optional, List

import numpy as np
import pandas as pd
import polars as pl
from polars import Expr


def concat_dataframes_from_dict(datas: Dict[str, pd.DataFrame]) -> pl.DataFrame:
    """拼接字典套DataFrame

    Parameters
    ----------
    datas : dict
        字典数据

    """
    return pl.concat([pl.from_dataframe(v).with_columns(stock_code=pl.lit(k)) for k, v in datas.items()])


def cast_datetime(df: pl.DataFrame, col: pl.Expr = pl.col('time')) -> pl.DataFrame:
    """转换时间列

    Parameters
    ----------
    df : pl.DataFrame
        polars DataFrame
    col : pl.Expr
        时间列。可以同时转换多个列，如：pl.col('time', 'open_dt', 'close_dt')

    """
    return df.with_columns(col.cast(pl.Datetime(time_unit="ms", time_zone="Asia/Shanghai")))


def arr_to_pl(arr: np.ndarray, col: Expr = pl.col('time')) -> pl.DataFrame:
    """numpy数组转polars DataFrame

    Parameters
    ----------
    arr : np.ndarray
        numpy数组
    col : pl.Expr
        时间列。可以同时转换多个列，如：pl.col('time', 'open_dt', 'close_dt')

    """
    return cast_datetime(pl.from_numpy(arr), col)


def concat_intraday(df1: Optional[pl.DataFrame], df2: pl.DataFrame,
                    by1: str = 'stock_code', by2: str = 'time',
                    by3: str = 'duration') -> pl.DataFrame:
    """日内分钟合并，需要排除重复

    数据是分批到来的，所以合成K线也是分批的，但很有可能出现不完整的数据，用duration来排除重复数据,只选最大的

    1. 前一DataFrame后期数据不完整
    2. 后一DataFrame前期数据不完整
    3. 前后DataFrame有重复数据

    Parameters
    ----------
    df1 : pl.DataFrame
        前一DataFrame
    df2 : pl.DataFrame
        后一DataFrame
    by1 : str
        分组字段
    by2 : str
        排序字段
    by3 : str
        去重字段

    """
    if df1 is None:
        return df2

    df = pl.concat([df1, df2], how='vertical')
    return df.sort(by1, by2, by3).unique(subset=[by1, by2], keep='last', maintain_order=True)


def get_common_elements(list1: List[str], list2: List[str]) -> List[str]:
    """获取两个列表的共同元素，保持原始顺序"""
    # 使用集合找到共同元素
    common_set = set(list1) & set(list2)
    # 保持原始顺序
    return [x for x in list1 if x in common_set]


def concat_interday(df1: Optional[pl.DataFrame], df2: pl.DataFrame) -> pl.DataFrame:
    """日间线合并，不会重复，但格式会有偏差"""
    if df1 is None:
        return df2
    # print(df1.columns)
    # print(df2.columns)
    cols = get_common_elements(df1.columns, df2.columns)
    return pl.concat([df1.select(*cols), df2.select(*cols)], how="vertical")


def calc_factor1(df: pl.DataFrame,
                 by1: str = 'stock_code', by2: str = 'time',
                 close: str = 'close', pre_close: str = 'pre_close') -> pl.DataFrame:
    """计算复权因子，乘除法。使用交易所发布的昨收盘价计算

    Parameters
    ----------
    df : pl.DataFrame
        数据
    by1 : str
        分组字段
    by2 : str
        排序字段
    close : str
        收盘价字段
    pre_close : str
        昨收盘价字段

    Notes
    -----
    不关心是否真发生了除权除息过程，只要知道前收盘价和收盘价不等就表示发生了除权除息

    """
    df = (
        df
        .sort(by1, by2)
        .with_columns(
            factor1=(pl.col(close).shift(1, fill_value=pl.first(pre_close)) / pl.col(pre_close)).round(8).over(by1, order_by=by2))
        .with_columns(factor2=(pl.col('factor1').cum_prod()).over(by1, order_by=by2))
    )
    return df


def calc_factor2(df: pl.DataFrame,
                 by1: str = 'stock_code', by2: str = 'time',
                 close: str = 'close', pre_close: str = 'pre_close') -> pl.DataFrame:
    """计算复权因子，加减法。使用交易所发布的昨收盘价计算

    Parameters
    ----------
    df : pl.DataFrame
        数据
    by1 : str
        分组字段
    by2 : str
        排序字段
    close : str
        收盘价字段
    pre_close : str
        昨收盘价字段

    Notes
    -----
    不关心是否真发生了除权除息过程，只要知道前收盘价和收盘价不等就表示发生了除权除息

    """
    df = (
        df
        .sort(by1, by2)
        .with_columns(
            factor1=(pl.col(close).shift(1, fill_value=pl.first(pre_close)) - pl.col(pre_close)).round(8).over(by1, order_by=by2))
        .with_columns(factor2=(pl.col('factor1').cum_sum()).over(by1, order_by=by2))
    )
    return df
