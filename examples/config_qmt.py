import pickle

from xtquant import xtdata


def update_sector():
    # 开盘前需要先更新板块数据，因为会有新股上市
    try:
        print("开始更新板块数据。长时间无反应，建议手工中断，从历史中获取板块数据")
        xtdata.download_sector_data()
        print("结束更新板块数据")
        # 没有 京市A股 沪深风险警示 沪深退市整理
        xtdata.get_sector_list()

        G = Exception()
        G.沪深A股 = xtdata.get_stock_list_in_sector("沪深A股")
        G.沪深指数 = xtdata.get_stock_list_in_sector("沪深指数")
        G.沪深基金 = xtdata.get_stock_list_in_sector("沪深基金")
        G.科创板 = xtdata.get_stock_list_in_sector("科创板")
        G.创业板 = xtdata.get_stock_list_in_sector("创业板")

        with open("download_sector_data.pkl", "wb") as f:
            pickle.dump(G, f)
    except KeyboardInterrupt as e:
        print("手工中断。从历史文件中获取，可能过期没有新上市品种")
        with open("download_sector_data.pkl", "rb") as f:
            G = pickle.load(f)

    print(
        f"沪深A股:{len(G.沪深A股)}, 沪深指数:{len(G.沪深指数)}, 沪深基金:{len(G.沪深基金)}, 科创板:{len(G.科创板)}, 创业板:{len(G.创业板)},")
    return G
