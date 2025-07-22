from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from npyt import NPYT
from numba import njit
from xtquant import xtconstant, xtdata
from xtquant.xtconstant import *  # noqa

from examples.config import TOTAL_ASSET
from qmt_quote.enums import SizeType, BoardType
from qmt_quote.utils import get_board_type
from qmt_quote.utils_qmt import get_instrument_detail_wrap


def to_dict(obj):
    """将xtquant对象转换为字典，排除私有属性和以m_开头的属性"""
    return {attr: getattr(obj, attr) for attr in dir(obj) if not attr.startswith(("__", "m_"))}


def objs_to_dataframe(objs):
    """对象列表转换为DataFrame"""
    return pd.DataFrame.from_records([to_dict(o) for o in objs])


@njit
def adjust_price_1(is_buy: bool, priority: int, offset: int,
                   bid_1: float, ask_1: float,
                   last_price: float = 0.0, pre_close: float = 0.0,
                   tick: float = 0.01) -> float:
    """激进或保守，偏移下单

    Parameters
    ----------
    is_buy:bool
    priority:int
        0 现价
        1 对价
        -1 挂价。撤单的概率高
    offset:int
        偏移。正数激进，负数保守，0表示保持不变
    bid_1:float
        卖一价
    ask_1:float
        买一价
    last_price:float
        最新价
    pre_close:float
        昨收价
    tick:float
        最小变动单位

    Notes
    -----
    买一价加一跳不等于卖一价

    """
    # print(last_price)
    if is_buy:
        maker, taker = bid_1, ask_1
    else:
        maker, taker = ask_1, bid_1

    if priority > 0:
        price = taker or maker or last_price or pre_close
    elif priority < 0:
        price = maker or taker or last_price or pre_close
    else:
        price = last_price or pre_close

    if is_buy:
        price += offset * tick
    else:
        price -= offset * tick
    return price


@njit
def adjust_price_2(is_buy: bool, board_type: BoardType, price: float,
                   bid_1: float, ask_1: float,
                   last_price: float = 0.0, pre_close: float = 0.0,
                   tick: float = 0.01) -> float:
    """价格笼子。集合竞价时没有价格笼子，可直接报涨跌停价

    （一）买入申报价格不得高于买入基准价格的102%和买入基准价格以上十个申报价格最小变动单位的孰高值；
    （二）卖出申报价格不得低于卖出基准价格的98%和卖出基准价格以下十个申报价格最小变动单位的孰低值。

    Parameters
    ----------
    is_buy:bool
    board_type
    price
        申报价格
    bid_1
        买一价
    ask_1
        卖一价
    last_price
        最新价
    pre_close
        昨收价
    tick
        最小变动单位

    Returns
    -------
    float
        调整后的价格

    """
    if is_buy:
        base = ask_1 or bid_1 or last_price or pre_close
    else:
        base = bid_1 or ask_1 or last_price or pre_close

    is_kcb = board_type == BoardType.KCB
    is_cyb = board_type == BoardType.CYB
    if is_kcb or is_cyb:
        if is_buy:
            return min(base * 1.02, price)
        else:
            return max(base * 0.98, price)

    tick10 = tick * 10
    is_bj = board_type == BoardType.BJ
    if is_bj:
        if is_buy:
            return min(max(base * 1.05, base + tick10), price)
        else:
            return max(min(base * 0.95, base - tick10), price)
    else:
        if is_buy:
            return min(max(base * 1.02, base + tick10), price)
        else:
            return max(min(base * 0.98, base - tick10), price)


@njit
def adjust_price_3(is_buy: bool, price: float,
                   limit_down: float = 0.0, limit_up: float = 99999.0,
                   ndigits: int = 1000) -> float:
    """买卖价不能超过涨跌停价

    Parameters
    ----------
    is_buy:bool
    price
        申报价格
    limit_down
        跌停价
    limit_up
        涨停价
    ndigits
        价格精度，100表示0.01股票, 1000表示0.001基金, 5表示0.2股指期货

    Returns
    -------
    float
        调整后的价格

    Notes
    -----
    TODO 新股上市第一天临时停牌规则要特别设计

    """
    # if is_buy:
    #     # 买入时价格向下靠拢
    #     price = math.floor(price * ndigits) / ndigits
    #     return max(min(limit_up, price), limit_down)
    # else:
    #     # 卖出时价格向上靠拢
    #     price = math.ceil(price * ndigits) / ndigits
    #     return min(max(limit_down, price), limit_up)
    if is_buy:
        # 买入时价格向下靠拢
        price = round(price * ndigits) / ndigits
        return max(min(limit_up, price), limit_down)
    else:
        # 卖出时价格向上靠拢
        price = round(price * ndigits) / ndigits
        return min(max(limit_down, price), limit_up)


def adjust_quantity(is_buy: bool, board_type: BoardType, quantity: int,
                    can_use_volume: int, tolerance: int = 10) -> int:
    """
    根据股票交易规则调整股票买卖数量

    Parameters
    ----------
    is_buy:bool
        是否买入
    board_type: BoardType
        股票板块类型
    quantity:int
        申报数量
    can_use_volume:int
        可用数量. 已经排除了挂单卖出时的冻结frozen_volume
    tolerance:int
        容忍度。遇到数量差异时，是否调整数量。

    Returns
    -------
    int
        调整后的数量

    Notes
    -----
    科创板：单笔申报数量不小于200股，以1股为单位递增
    非科创板：买入时，数量向下取整到 100 的整数倍；卖出时，数量向上取整到 100 的整数倍

    """
    is_kcb = board_type == BoardType.KCB
    is_cyb = board_type == BoardType.CYB

    if is_buy:
        if is_kcb:
            # 科创板：单笔申报数量不小于200股，以1股为单位递增
            if quantity < 200 - tolerance:
                # 少于一定数量，不买了
                quantity = 0
            else:
                # 最低200股
                quantity = max(int(quantity), 200)
        else:
            # 买入时，数量向下取整到 100 的整数倍
            quantity = (quantity + tolerance) // 100 * 100
    else:
        # C  Q
        if quantity + tolerance >= can_use_volume:
            # 数量大于可用数量，全部卖出
            quantity = can_use_volume
        elif is_kcb:
            if quantity + tolerance >= 200:
                quantity = max(int(quantity), 200)
            else:
                quantity = 0
        else:
            # 1 Q 2 C 3
            if quantity + tolerance >= 100:
                quantity = (quantity + tolerance) // 100 * 100
            else:
                quantity = 0

    # 不能超过最大下单数，否则报错
    if is_kcb:
        quantity = min(quantity, 100000)
    elif is_cyb:
        quantity = min(quantity, 300000)
    else:
        quantity = min(quantity, 1000000)

    return quantity


def cancel_orders(trader, account, orders: Optional[pd.DataFrame] = None,
                  direction: int = 0,
                  strategy_name: str = None, order_remark: str = None,
                  do_async: bool = False) -> pd.DataFrame:
    """撤单

    Parameters
    ----------
    trader
    account
    orders: pd.DataFrame
        - order_id:int (required)
        - order_status:int (optional)
        - direction:int (optional)

    direction
        0 全部
        1 买
        -1 卖
    strategy_name:str
        策略名称
    order_remark:str
        备注
    do_async:bool
        是否异步撤单。异步撤单可能会引发流控

    Returns
    -------
    pd.DataFrame
        - seq

    References
    ----------
    https://dict.thinktrader.net/nativeApi/xttrader.html?id=x3GDHP#%E5%A7%94%E6%89%98%E7%8A%B6%E6%80%81-order-status

    Warnings
    --------
    当前连续撤单失败次数: 0次，距离上次发起撤单间隔0秒
    (当连续撤单失败次数大于等于3次或者距离上次撤单发起不超过2秒时，系统将直接反馈撤单失败)


    """
    if orders is None:
        orders = trader.query_stock_orders(account, cancelable_only=True)
        orders = objs_to_dataframe(orders)
    else:
        # 55部成 52部成待撤, 这两状态有什么区别
        orders = orders.query(f'(order_status==@ORDER_PART_SUCC) | (order_status<=@ORDER_PARTSUCC_CANCEL)')

    if orders.empty:
        return orders

    if direction != 0:
        if direction > 0:
            orders = orders.query(f'direction==@DIRECTION_FLAG_BUY')
        elif direction < 0:
            orders = orders.query(f'direction==@DIRECTION_FLAG_SELL')

    if strategy_name is not None:
        orders = orders.query(f'strategy_name==@strategy_name')
    if order_remark is not None:
        orders = orders.query(f'order_remark==@order_remark')

    # 记录请求序列号
    if 'seq' not in orders.columns:
        orders['seq'] = 0
    for i, row in orders.iterrows():
        if do_async:
            # TODO 不能撤太快，会引发流控
            orders.loc[i, 'seq'] = trader.cancel_order_stock_async(account, row.order_id)
        else:
            # 换成异步就行？
            orders.loc[i, 'seq'] = trader.cancel_order_stock(account, row.order_id)
    return orders


def before_market_open(G) -> pd.DataFrame:
    """下载板块数据，获取当天涨跌停价

    Parameters
    ----------
    G: object
        全局变量对象。此函数中用于记录板块数据

    Returns
    -------
    pd.DataFrame
        - is_kcb:bool
        - is_st:bool
        - is_delisting:bool
        - DownStopPrice:float
        - UpStopPrice:float

    Warnings
    --------
    遇到风险股票时，公告出来了，但更名还是晚一步。所以需要使用其他手段获取并标记

    """
    # 先下载板块数据
    xtdata.download_sector_data()
    # 没有 京市A股 沪深风险警示 沪深退市整理
    xtdata.get_sector_list()

    G.沪深A股 = xtdata.get_stock_list_in_sector("沪深A股")
    G.科创板 = xtdata.get_stock_list_in_sector("科创板")
    G.创业板 = xtdata.get_stock_list_in_sector("创业板")
    G.沪深基金 = xtdata.get_stock_list_in_sector("沪深基金")

    details1 = get_instrument_detail_wrap(G.沪深A股)
    details1['board_type'] = details1.index.map(get_board_type)
    # 由于 沪深风险警示 沪深退市整理, 数据为空，只好从股票名字中获取
    details1['is_st'] = details1['InstrumentName'].str.contains('ST')
    details1['is_delisting'] = details1['InstrumentName'].str.contains('退')

    details2 = get_instrument_detail_wrap(G.沪深基金)
    details2['board_type'] = details2.index.map(get_board_type)
    # 由于 沪深风险警示 沪深退市整理, 数据为空，只好从股票名字中获取
    details2['is_st'] = False
    details2['is_delisting'] = False
    return pd.concat([details1, details2])


def send_orders_1(trader, account, details, npyt_obj: NPYT):
    """下单前准备工作第1步

    1. 获取可平持仓数量
    2. 获取涨跌价格

    Parameters
    ----------
    trader
    account
    details: pd.DataFrame 股票详情。来自于get_instrument_detail
        - stock_code:str (required)
        - board_type:int (required)
        - DownStopPrice:float (required)
        - UpStopPrice:float (required)
    npyt_obj: NPYT 日行情内存映射文件

    Returns
    -------
    pd.DataFrame
        - can_use_volume:int (required)
        - bidPrice_1:float (required)
        - askPrice_1:float (required)
        - close:float (required)
        - pre_close:float (required)

    Notes
    -----
    返回的是全部股票，使用前需要过滤

    """
    # 从内存映射文件中读取日线数据，内有买卖一价
    end = npyt_obj.end()
    assert end > 0, 'No data in memory mapping file.'

    ticks = pd.DataFrame(npyt_obj.tail(TOTAL_ASSET)).drop_duplicates(
        subset=['stock_code'], keep='last').set_index('stock_code')
    # volume与Position中重名，所以改一下
    ticks.rename(columns={'volume': 'VOLUME'}, inplace=True)
    # 合并涨跌停
    df = pd.merge(details, ticks, left_index=True, right_index=True, how='left')
    # 获取可卖出持仓数量
    if (trader is not None) and (account is not None):
        # 查询持仓
        positions = trader.query_stock_positions(account)
        if len(positions) > 0:
            positions = objs_to_dataframe(positions).set_index('stock_code')
            # ETF基金被跳过了
            df = pd.merge(df, positions, left_index=True, right_index=True, how='left')
            # 可平数量
            df['can_use_volume'] = df['can_use_volume'].fillna(0).astype(int)
            # 持仓数量
            df['volume'] = df['volume'].fillna(0).astype(int)

    # position为空，将重要地方补全，后面会用到
    if 'can_use_volume' not in df.columns:
        df['can_use_volume'] = 0
    if 'volume' not in df.columns:
        df['volume'] = 0

    return df


def send_orders_2(orders: pd.DataFrame,
                  new_orders: pd.DataFrame,
                  size: float = 0,
                  or_volume: bool = True) -> pd.DataFrame:
    """过滤要交易的股票，并设置size

    1. 因子结果所指定的股票，一般是要买入的股票
    2. 持仓中的股票，一般是要卖出的股票或调整的股票

    Parameters
    ----------
    orders: pd.DataFrame
        - volume:int (required)
    new_orders: pd.DataFrame
        - size:float (required)

    Notes
    -----
    本代码只是演示如何调整size，用户应当按自己的需求另外创建处理函数

    """
    # 设置size
    new_orders['size'] = size
    # 这一步会导致部分数据由int变成了float
    orders = pd.merge(orders, new_orders, left_index=True, right_on="stock_code", how='left')
    orders['strategy_id'] = orders['strategy_id'].fillna(0).astype(int)

    if or_volume:
        orders = orders[(orders['size'].notna()) | (orders['volume'] > 0)].copy()
    else:
        orders = orders[orders['size'].notna()].copy()

    return orders


def send_orders_3(trader, account, orders: pd.DataFrame, size_type: SizeType) -> pd.DataFrame:
    """下单前准备工作第3步。没有第2步，因为第2步是用户自己设置的

    1. 委托量计算
    2. 方向计算
    
    Parameters
    ----------
    trader
    account
    orders: pd.DataFrame
        - stock_code:str (required)
        - board_type:int (required)
        - price:float (required)
        - can_use_volume:int (required)
        - quantity:int (required)
        - is_buy:bool (required)
    size_type:str

    
    Returns
    -------
    pd.DataFrame
        - quantity:int (required)
        
    Notes
    -----
    算目标市值是用的最新价然后倒推的所需要手数
    但冻结资金是柜台按报单价算的，如果按涨停价报，报单量可能要缩减，防止资金不足

    计算可平仓数量时，需要持仓量数据

    References
    ----------
    https://github.com/wukan1986/LightBT/blob/main/lightbt/portfolio.py#L283
    
    """
    if orders.empty:
        return orders

    # 查可用资金
    asset = to_dict(trader.query_stock_asset(account))
    total_asset = asset['total_asset']

    orders['size'] = orders['size'].fillna(0)
    orders['volume'] = orders['volume'].fillna(0)
    orders['close'] = orders['close'].fillna(0)

    if size_type == SizeType.TargetValueScale:
        orders['size'] = orders['size'] / orders['size'].abs().sum()
        size_type = SizeType.TargetValuePercent
    if size_type == SizeType.TargetValuePercent:
        orders['size'] = orders['size'] * total_asset
        size_type = SizeType.TargetValue

    if size_type == SizeType.TargetValue:
        # 没有行情的股要注意
        orders['size'] = orders['size'] - orders['close'] * orders['volume']
        size_type = SizeType.Value

    if size_type == SizeType.TargetAmount:
        # 这里不考虑可平手数
        orders['size'] = orders['size'] - orders['volume']
        size_type = SizeType.Amount

    if size_type == SizeType.Value:
        # TODO 这里是否要修改成报单价？冻结资金和可开手术其实是按报单价算的
        orders['size'] = orders['size'] / orders['close']
        size_type = SizeType.Amount

    if size_type == SizeType.Amount:
        orders['is_buy'] = orders['size'] >= 0
        orders['size'] = orders['size'].abs()

    # 先卖后买，先平后开
    orders.sort_values('is_buy', inplace=True)

    return orders


def send_orders_4(orders: pd.DataFrame, priority: int, offset: int, is_auction: bool) -> pd.DataFrame:
    """下单前准备工作第4步

    1. 调整下单价格
    2. 价格笼子调整
    3. 涨跌停调整

    Parameters
    ----------
    orders: pd.DataFrame
        - stock_code:str (required)
        - DownStopPrice:float (required)
        - UpStopPrice:float (required)
        - bidPrice_1:float (required)
        - askPrice_1:float (required)
        - close:float (required)
        - pre_close:float (required)
        - is_buy:bool (required)
    priority:int
        报价激进或保守
    offset:int
        报价偏移
    is_auction:bool
        是否集合竞价时段。集合竞价时，没有价格笼子

    Returns
    -------
    pd.DataFrame
        - price:float (required)

    """
    if orders.empty:
        return orders

    # orders = orders[orders['stock_code'] == '002750.SZ'].copy()

    # 根据需求设置下单价格
    orders['price'] = orders.apply(
        lambda x: adjust_price_1(x.is_buy, priority, offset, x.bidPrice_1, x.askPrice_1, x.close, x.pre_close,
                                 tick=x.PriceTick), axis=1)
    if not is_auction:
        # 价格笼子调整
        orders['price'] = orders.apply(
            lambda x: adjust_price_2(x.is_buy, x.board_type, x.price, x.bidPrice_1, x.askPrice_1, x.close,
                                     x.pre_close, tick=x.PriceTick), axis=1)
    # 涨跌停调整
    orders['price'] = orders.apply(
        lambda x: adjust_price_3(x.is_buy, x.price, x.DownStopPrice, x.UpStopPrice, ndigits=1000), axis=1)

    return orders


def send_orders_5(trader, account, orders: pd.DataFrame, order_remark: str, debug: bool = True) -> pd.DataFrame:
    """下单第5步

    1. 委托量调整
    2. 下单

    Parameters
    ----------
    trader
    account
    orders: pd.DataFrame
        - stock_code:str (required)
        - board_type:int (required)
        - price:float (required)
        - can_use_volume:int (required)
        - size:float (required)
        - is_buy:bool (required)
    order_remark:str
        备注
    debug:bool
        调试，只打印不下单

    Returns
    -------
    pd.DataFrame
        - seq
        - order_type
        - order_volume

    """
    # 过滤掉不交易的
    orders = orders[orders['size'] > 0].copy()
    if orders.empty:
        return orders

    orders['order_type'] = np.where(orders['is_buy'], xtconstant.STOCK_BUY, xtconstant.STOCK_SELL)
    # 委托量调整
    orders['order_volume'] = orders.apply(
        lambda x: adjust_quantity(x.is_buy, x.board_type, x['size'], x.can_use_volume, tolerance=10), axis=1)
    # 过滤掉不交易的
    orders = orders[orders['order_volume'] > 0].copy()
    if orders.empty:
        return orders

    orders['seq'] = 0
    orders.reset_index(inplace=True)
    print('=' * 60)
    orders['close_dt_y'] = orders['close_dt_y'].fillna(orders['close_dt_x'])
    for i, v in orders.iterrows():
        strategy_name = str(v['strategy_id'])
        value = v.price * v.order_volume
        print(
            f'close_dt={datetime.fromtimestamp(v.close_dt_y / 1000)},stock_code={v.stock_code},is_buy={v.is_buy},price={v.price},order_volume={v.order_volume},{strategy_name=},{order_remark=},{value=}')

        if not debug:
            orders.loc[i, 'seq'] = trader.order_stock_async(account, v.stock_code, v.order_type, v.order_volume,
                                                            xtconstant.FIX_PRICE, v.price, strategy_name, order_remark)
    return orders
