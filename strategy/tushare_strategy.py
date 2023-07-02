import os
import threading
from datetime import datetime, timedelta
AMOUNT_BILLION = 1000000000

from backtest_engine import BacktestEngine
from backtest_thread import BacktestThread

if __name__ == "__main__":
    #create data dir
    if not os.path.exists('data/'):
        os.mkdir('data/')
    engine = BacktestEngine(False,30)
    # stocks = engine.tushare_tool.get_all_stocks()
    stocks = engine.tushare_tool.get_bak_basic()
    today = datetime.today()
    end_date = today.strftime('%Y%m%d')
    start_day = ( datetime.strptime(end_date,'%Y%m%d') - timedelta(1000)).strftime('%Y%m%d')
    engine.tushare_tool.download_daily_line(stocks)
    engine.tushare_tool.download_moneyflow()
    exit(0)

    ma_array = [30]
    threads = {}
    for ma_days in ma_array:
        thread = BacktestThread(ma_days)
        threads[ma_days] = thread
        thread.start()

    for thread_id in threads:
        threads[thread_id].join()
