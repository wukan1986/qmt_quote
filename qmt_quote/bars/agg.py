"""
k线上其实有3个时间：
1. 第一个tick的时间
2. 最后一个tick的时间
3. 标签时间

区域内时间是左闭右开
[left, right)

行业软件中，标签时间有两种：left和right
国内很多软件中，分钟标签时间都是right。

但我个人偏向用left。因为所有软件的日线其实是left标签。分钟与日线标签统一更合适
left标签更容易处理。因为
1. left=time//60*60
2. right=time//60*60+60
而1分钟转5分钟时
1. left=time//300*300
2. right=(time-60)//300*300+300

right标签也有优势，因为可以直接用当前时间与标签时间比较，得到当前k线是否结束

在实际应用中，left与right都可以，只要求
1. 在分钟转换前，同一条K线的标签是统一的即可
2. 在横截面比较时，多个K线的标签是统一的即可

"""
import polars as pl


def ticks_to_minute(df: pl.DataFrame, every: str = "1m", closed: str = "left", label: str = "right") -> pl.DataFrame:
    """tick转分钟数据

    Parameters
    ----------
    df
    every:str
        周期，例如：1m, 5m, 15m, 30m, 60m
    closed: str
        周期的闭合方式，例如：left, right
    label: str
        标签的位置，例如：left, right

    Notes
    -----
    1. 只能日内tick数据转成分钟数据。多日tick数据需要先按日分组合
    2. 通过最新价格计算最高低价。凑不出日线最高低价

    """
    # 成交量和成交额转成K线数据前要做差分
    df = (
        df
        .sort("stock_code", "volume")
        .with_columns(
            volume_diff=pl.col("volume").diff().fill_null(pl.col("volume")).over("stock_code", order_by="volume"),
            amount_diff=pl.col("amount").diff().fill_null(pl.col("amount")).over("stock_code", order_by="volume"),
        )
        .sort("stock_code", "time")
        .group_by_dynamic(
            "time",
            every=every,
            closed=closed,
            label=label,
            group_by=[
                "stock_code",
            ],
        )
        .agg(
            open_dt=pl.first("time"),
            close_dt=pl.last("time"),
            open=pl.first("lastPrice"),
            high=pl.max("lastPrice"),
            low=pl.min("lastPrice"),
            close=pl.last("lastPrice"),
            volume=pl.sum("volume_diff").cast(pl.UInt64),
            amount=pl.sum("amount_diff"),
            pre_close=pl.first("lastClose"),
        )
        .with_columns(
            pre_close=pl.col("close").shift(1, fill_value=pl.col("pre_close")).over('stock_code', order_by='time'),
        )
        .with_columns(duration=pl.col("close_dt") - pl.col("open_dt"))
    )

    return df


def ticks_to_day(df: pl.DataFrame) -> pl.DataFrame:
    """ tick转日数据

    Notes
    -----
    只能日内tick数据转成日数据。多日tick数据需要先按日分组合

    """
    df = (
        df
        .sort("stock_code", "time")
        .group_by_dynamic(
            "time",
            every="1d",
            closed="left",
            label="left",
            group_by=[
                "stock_code",
            ],
        )
        .agg(
            open_dt=pl.first("time"),
            close_dt=pl.last("time"),
            open=pl.first("open"),
            high=pl.max("high"),
            low=pl.min("low"),
            close=pl.last("lastPrice"),
            volume=pl.last("volume"),
            amount=pl.last("amount"),
            pre_close=pl.first("lastClose"),
        ).with_columns(duration=pl.col("close_dt") - pl.col("open_dt"))
    )
    return df


def convert_1m_to_5m(df: pl.DataFrame, every: str = "5m", closed: str = "right", label: str = "right") -> pl.DataFrame:
    """1分钟线转5分钟线

    Parameters
    ----------
    df
    every:str
        周期
    closed:str
        周期闭合
    label:str
        周期标签

    """
    df = (
        df
        .sort("stock_code", "time")
        .group_by_dynamic(
            "time",
            every=every,
            closed=closed,
            label=label,
            group_by=[
                "stock_code",
            ],
        )
        .agg(
            open_dt=pl.first("time"),
            close_dt=pl.last("time"),
            open=pl.first("open"),
            high=pl.max("high"),
            low=pl.min("low"),
            close=pl.last("close"),
            volume=pl.sum("volume"),
            amount=pl.sum("amount"),
            total_volume=pl.last("total_volume"),
            total_amount=pl.last("total_amount"),
            pre_close=pl.first("pre_close"),
            last_close=pl.first("last_close"),
            # 历史1分钟有这个字典，5分钟要补上
            suspendFlag=pl.last("suspendFlag"),
        )
    )
    return df
