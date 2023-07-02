import akshare as ak
class aktool:
    def __init__(self):
        pass

    #淘股吧热票
    def download_hot_tgb(self):
        return ak.stock_hot_tgb();

    #问财排行
    def download_hot_rank_wc(self):
        return ak.stock_hot_rank_wc()

    #东方财富排行
    def download_hot_rank_em(self):
        return ak.stock_hot_rank_em()

    def download_stock_hot_search_baidu(self,trade_date):
        return ak.stock_hot_search_baidu(symbol="A股", date=trade_date, time="今日");

    def download_hot_rank(self):
        # tgb_rank = self.download_hot_tgb()
        # wc_rank = self.download_hot_rank_wc()
        em_rank = self.download_hot_rank_em()
        em_rank.rename(columns={'当前排名':'rank','代码':'code','股票名称':'stock_name'},inplace=True)
        em_rank = em_rank[['rank','code','stock_name']]
        return em_rank


