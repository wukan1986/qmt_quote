"""
D盘：QMT安装目录和数据
F盘：备份数据使用
M盘：内存盘，也是内存映射文件存放盘。盘后将归档到F盘。重启后自动清空

内存大于等于64G，建议使用内存盘。至少分配12G

ImDisk Toolkit
https://imdisk-toolkit.en.lo4d.com/download

"""

# =====历史行情======
# TODO 历史数据目录，使用实盘QMT标准版，可以手工下载历史数据，然后在miniquant中读出,也可用第三方数据
DATA_DIR = r"D:\国泰海通方舟量化版交易端\datadir"

# TODO: 历史数据文件
HISTORY_STOCK_1d = r"F:\stock_1d.parquet"
HISTORY_INDEX_1d = r"F:\index_1d.parquet"
HISTORY_STOCK_1m = r"F:\stock_1m.parquet"
HISTORY_INDEX_1m = r"F:\index_1m.parquet"
HISTORY_STOCK_5m = r"F:\stock_5m.parquet"

# =====实时行情======
# TODO：行数，一定每天接收最大数据量上再扩充一些，防止溢出。溢出时会直接崩溃退出
# 5000多只股票,500多个指数
TOTAL_ASSET = 7000

# 1分钟数据量。股票3秒更新一次
TICKS_PER_MINUTE = int(60 / 3 * TOTAL_ASSET)
TICKS_PER_DAY = TICKS_PER_MINUTE * 60 * 4
BARS_PER_MINUTE = TOTAL_ASSET  # 1分钟bar数量
BARS_PER_DAY = TOTAL_ASSET * 60 * 4

# 数据长度
# 注意：除了tick数据，其他数据转换都是numba实现的，没有边界检查，超范围会崩溃
TOTAL_1t = TICKS_PER_DAY * 1  # 1天
TOTAL_1m = BARS_PER_DAY * 10  # 1分钟10天
TOTAL_5m = BARS_PER_DAY // 5 * 10  # 5分钟*10天
TOTAL_1d = TOTAL_ASSET * 240  # 日线240天

# TODO: 文件名，根据实际情况修改为自己的文件名。可放到内存盘
# 数据
FILE_d1t = r"M:\d1t.npy"
FILE_d1m = r"M:\d1m.npy"
FILE_d5m = r"M:\d5m.npy"
FILE_d1d = r"M:\d1d.npy"

# 备份目录
BACKUP_DIR = r"F:\backup"

# =====交易设置======
# 信号
FILE_s1t = r"M:\s1t.npy"  # 顺序记录插入的信号
FILE_s1d = r"M:\s1d.npy"  # 日频信号，用与下单

# =====实盘行情信息======
FILE_details = r"M:\details.parquet"

# TODO 交易目录，miniquant版
USERDATA_DIR = r"D:\迅投极速交易终端 睿智融科版\userdata_mini"
# TODO: 交易账号
ACCOUNT = "2025727"
