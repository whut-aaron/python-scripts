from strategy_base import StrategyBase
from trend_tool import TrendTool
class FallBackStrategy(StrategyBase):
    def __init__(self,ma_days,min_days):
        StrategyBase.__init__(self,'FallbackStrategy',ma_days,min_days)
        self.trend_tool = TrendTool(ma_days,min_days)

    def run(selfdf, stock_id, total_share, backtest_date):
        pass

