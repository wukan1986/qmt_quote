from numba import njit


@njit(cache=True)
def get_label_stock_1m(t: int, tz: int = 3600 * 8) -> int:
    """1分钟标签

    9点25之前的数据丢弃
    9点25到9点31之前的数据标签为9点30
    11点29到11点31之前的数据标签为11点29
    14点59到15点01之前的数据标签为14点59
    16点00之后的盘后交易丢弃
    """
    t_1d = (t + tz) // 86400 * 86400 - tz
    t_1m = t // 60 * 60
    s = t_1m - t_1d

    if s < 33900:  # 9:25
        return 0
    if s > 57600:  # 16:00
        return 0
    if s < 34200:  # 9:30
        return t_1d + 34200
    if s == 41400:  # 11:30
        return t_1m - 60  # 11:29
    if s == 54000:  # 15:00
        return t_1m - 60  # 14:59
    return t_1m


@njit(cache=True)
def get_label_stock_5m(t: int, tz: int = 3600 * 8) -> int:
    """5分钟标签"""
    t_1d = (t + tz) // 86400 * 86400 - tz
    t_1m = t // 300 * 300
    s = t_1m - t_1d

    if s < 33900:  # 9:25
        return 0
    if s > 57600:  # 16:00
        return 0
    if s < 34200:  # 9:30
        return t_1d + 34200
    if s == 41400:  # 11:30
        return t_1m - 300  # 11:25
    if s == 54000:  # 15:00
        return t_1m - 300  # 14:55
    return t_1m


@njit(cache=True)
def get_label_stock_15m(t: int, tz: int = 3600 * 8) -> int:
    """15分钟标签"""
    t_1d = (t + tz) // 86400 * 86400 - tz
    t_1m = t // 900 * 900
    s = t_1m - t_1d

    if s < 33900:  # 9:25
        return 0
    if s > 57600:  # 16:00
        return 0
    if s < 34200:  # 9:30
        return t_1d + 34200
    if s == 41400:  # 11:30
        return t_1m - 900  # 11:15
    if s == 54000:  # 15:00
        return t_1m - 900  # 14:45
    return t_1m


@njit(cache=True)
def get_label_stock_30m(t: int, tz: int = 3600 * 8) -> int:
    """30分钟标签"""
    t_1d = (t + tz) // 86400 * 86400 - tz
    t_1m = t // 1800 * 1800
    s = t_1m - t_1d

    if s < 33900:  # 9:25
        return 0
    if s > 57600:  # 16:00
        return 0
    if s < 34200:  # 9:30
        return t_1d + 34200
    if s == 41400:  # 11:30
        return t_1m - 1800  # 11:00
    if s == 54000:  # 15:00
        return t_1m - 1800  # 14:30
    return t_1m


@njit(cache=True)
def get_label_stock_60m(t: int, tz: int = 3600 * 8) -> int:
    """60分钟标签

    9点30到10点29之间的标签为9点30
    10点30到11点30之间的标签为10点30
    13点00到13点59之间的标签为13点00
    14点00到15点00之间的标签为14点00
    """
    t_1d = (t + tz) // 86400 * 86400 - tz
    t_1m = t // 60 * 60
    s = t_1m - t_1d

    if s < 33900:  # 9:25
        return 0
    if s > 57600:  # 16:00
        return 0
    if s < 34200:  # 9:30
        return t_1d + 34200
    if s == 41400:  # 11:30
        return t_1m - 3600  # 10:30
    if s == 54000:  # 15:00
        return t_1m - 3600  # 14:00

    if s < 43200:  # 12:00
        return (t - 1800) // 3600 * 3600 + 1800
    else:
        return t // 3600 * 3600


@njit(cache=True)
def get_label_stock_120m(t: int, tz: int = 3600 * 8) -> int:
    """120分钟标签

    9点30到11点29之间的标签为9点30
    13点00到15点00之间的标签为13点00
    """
    t_1d = (t + tz) // 86400 * 86400 - tz
    t_1m = t // 60 * 60
    s = t_1m - t_1d

    if s < 33900:  # 9:25
        return 0
    if s > 57600:  # 16:00
        return 0
    if s < 43200:  # 12:00
        return t_1d + 34200  # 9:30
    else:
        return t_1d + 46800  # 13:00


@njit(cache=True)
def get_label_stock_12h(t: int, tz: int = 3600 * 8) -> int:
    """12小时标签
    """
    t_1d = (t + tz) // 86400 * 86400 - tz
    t_1m = t // 60 * 60
    s = t_1m - t_1d

    if s < 43200:  # 12:00
        return t_1d + 0  # 0:00
    else:
        return t_1d + 43200  # 12:00


@njit(cache=True)
def get_label_stock_1d(t: int, tz: int = 3600 * 8) -> int:
    """1日线标签

    由于想用日线实现tick数据快照的功能，所以不丢弃数据，直接返回当天的0点
    """
    return (t + tz) // 86400 * 86400 - tz


@njit(cache=True)
def get_label(t: int, bar_size: int, tz: int = 3600 * 8) -> int:
    """
    # 日线, 东八区处理
    label_1d = (curr_time + 3600 * 8) // 86400 * 86400 - 3600 * 8 - 86400

    日线处理一定要加时区
    """
    return (t + tz) // bar_size * bar_size - tz


@njit(cache=True)
def get_traded_minutes__0900_1130__1300_1500(t: int, tz: int = 3600 * 8) -> int:
    """获取已经交易了多少分钟"""
    t_1d = (t + tz) // 86400 * 86400 - tz
    t_1m = t // 60 * 60 + 60  # 多加1分钟
    s = t_1m - t_1d

    if s <= 34200:  # 9:30
        return 1
    if s >= 54000:  # 15:00
        return 240

    if s >= 41400:  # 11:30
        morning_minutes = 120
    elif s >= 34200:  # 9:30
        morning_minutes = int(s - 34200) // 60
    else:
        morning_minutes = 0

    if s >= 54000:  # 15:00
        afternoon_minutes = 120
    elif s >= 46800:  # 13:00
        afternoon_minutes = int(s - 46800) // 60
    else:
        afternoon_minutes = 0

    return morning_minutes + afternoon_minutes
