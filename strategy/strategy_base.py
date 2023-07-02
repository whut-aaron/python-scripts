class StrategyBase():
    def __init__(self,strategy_name,ma_days,min_days):
        self.ma_days = ma_days
        self.min_days = min_days
        self.ma_name = "ma" + str(self.ma_days)
        self.strategy_name=strategy_name
        pass

    def run(self, df, stock_id, total_share, backtest_date):
        pass

    def set_ma(self,ma_days):
        self.ma_days = ma_days
        self.ma_name = "ma" + str(self.ma_days)
