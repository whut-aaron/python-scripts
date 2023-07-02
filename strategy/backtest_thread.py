import threading
from datetime import datetime, timedelta
from backtest_engine import BacktestEngine
from bull_strategy import BullStrategy
from rebound_strategy import ReboundStrategy
from fall_strategy import FallBackStrategy
AMOUNT_BILLION = 1000000000
class BacktestThread(threading.Thread):
    def __init__(self, ma_days):
        self.ma_days = ma_days
        super().__init__(name="backtest")

    def run(self) -> None:

        engine = BacktestEngine(False,self.ma_days)
        today = datetime.today()
        # end_date = today.strftime('%Y%m%d') - timedelta(1)
        end_date = (today - timedelta(1)).strftime('%Y%m%d')
        start_day = (datetime.strptime(end_date, '%Y%m%d') - timedelta(1000)).strftime('%Y%m%d')

        trend_days = 180
        strategy = BullStrategy(self.ma_days,trend_days)
        # engine.add_strategy('BullStrategy', strategy)
        engine.add_strategy('TrendStrategy', ReboundStrategy(self.ma_days, trend_days))
        # engine.add_strategy('FallBackStrategy',FallBackStrategy(self.ma_days, trend_days))

        engine.backtest_result_file = './data/' + str(self.ma_days)+ '.csv'
        engine.run_backtest(start_day, end_date)
        # start_day = (datetime.strptime(end_date, '%Y%m%d') - timedelta(trend_days * 3)).strftime('%Y%m%d')
        # engine.run(start_day,end_date)
        engine.ctx.orders.clear()
