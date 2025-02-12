# qmt_quote

迅投MiniQMT全推行情记录

将底层的tick全推行情通过回调转发记录到内存映射文件，用户可以实时读取并转换成K线数据

注意：

1. QMT可以手动下载Tick数据
2. 再通过`get_local_data([], ['000001.SZ'], period='tick', data_dir=r'D:\e海方舟-量化交易版\datadir')`读取

## 基础用法

1. 安装`QMT`
2. 安装`python/conda`。版本不能太低，截至2025年2月，推荐`python3.12`
3. 在虚拟环境中安装`xtquant`
4. 在虚拟环境中`pip install -r requirements.txt`
5. 修改`config.py`中的配置。如：
    - TOTAL_STOCK: 股票的总记录条数，**一定要预留足够的空间**，否则溢出报错
    - FILE_INDEX: 指数数据文件路径。会维护2个文件。一个存数据，一个记录最新位置
    - TICK_STOCK: 股票数据格式。一般不需要修改
    - OVERLAP_STOCK: 股票3分钟收到的总TICK数量,合并1分钟数据时的重叠区
6. 运行`python subscribe.py`, 转存全推行情。需要在开盘前运行，否则错失数据

## 进阶用法

1. 运行`query.py`，可实时查看`K线数据`和`tick数据`。这也是增量取数据的方式。可在策略中使用
2. 每次切换环境、运行脚本比较麻烦，可以直接双击运行`run.bat`
3. `archive.py`收盘后归档

## 注意

1. 每天开盘前都需要先删除数据文件，否则数据是接后面添加的，会导致运行一段时间后溢出
2. 目前只能一次处理一天的数据，多天一次处理需要修改K线数据的逻辑
3. 开始运行是会接收一次全推数据，要过滤
4. 接收数据时，小节收盘时，会延迟几秒钟还收到数据，可能要处理

## 技巧

1. 运行`run.bat`后非常担心不小心将窗口关闭。`Windows Terminal`可以再开一个选项卡，这样多个选项卡关闭时会提示