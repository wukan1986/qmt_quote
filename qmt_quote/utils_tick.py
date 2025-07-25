from typing import Dict, Any

import pandas as pd

from qmt_quote.enums import InstrumentType


def ticks_to_dataframe(datas: Dict[str, Dict[str, Any]],
                       now: int, index_name: str = 'stock_code',
                       level: int = 0, depths=["askPrice", "bidPrice", "askVol", "bidVol"],
                       type: InstrumentType = -1,
                       ) -> pd.DataFrame:
    """字典嵌套字典 转 DataFrame

    在全推行情中，接收到的嵌套字典转成DataFrame

    Parameters
    ----------
    datas : dict
        字典数据
    now :int
        当前时间戳
    index_name
        索引名，资产名
    level : int
        行情深度
    depths
        深度行情列名
    type:
        类型

    Returns
    -------
    pd.DataFrame

    """
    df = pd.DataFrame.from_dict(datas, orient="index")
    df["now"] = now
    df["type"] = type
    # df["avg_price"] = df["amount"] / df["pvolume"]

    # 行情深度
    for i in range(level):
        j = i + 1
        new_columns = [f'{c}_{j}' for c in depths]
        df[new_columns] = df[depths].map(lambda x: x[i])

    # 索引股票代码，之后要用
    df.index.name = index_name
    return df
