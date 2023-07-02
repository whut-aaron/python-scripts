from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo
from TusharePandasData import TusharePandasData
from tushare_tool import TushareTool
import datetime
import backtrader as bt
import pandas as pd
from MovingAverageStrategy import TestStrategy
from TrendBreakStrategy import TrendBreakStrategy
from StockACommissionScheme import StockAcommissionScheme


if __name__ == '__main__':
    # 股票池

    #stock basics
    tushare_tool = TushareTool() ;
    stock_basics = tushare_tool.get_bak_basic();
    stock_list = stock_basics['ts_code'].drop_duplicates()


    # 读取股票数据并转换为Pandas DataFrame
    data = pd.read_csv('/python-scripts/strategy/data/k_line.csv', nrows=3000000)  # 将路径替换为你的CSV文件路径
    data['datetime'] = pd.to_datetime(data['trade_date'], format='%Y%m%d')

    data = data.set_index('datetime')
    data.sort_index(ascending=True, inplace=True)
    data = data.rename(columns={'vol': 'volume'})
    data = data[['open', 'close', 'high', 'low', 'volume', 'amount', 'ts_code','pct_chg']]
    data['openinterest'] = data['amount']
    data['pe'] = 1
    data['pb'] = 2

    # Create a cerebro entity
    cerebro = bt.Cerebro()
    cerebro.addstrategy(TrendBreakStrategy)
    count = 0

    stock_basic = tushare_tool.get_bak_basic()
    count = 0
    for index, row in stock_basic.iterrows():
        stock_id = row['ts_code']
        total_share = row['total_share']
        float_share = row['float_share']
        stock_data = data[data['ts_code'] == stock_id]
        close_price = stock_data.iloc[-1]['close']
        market_value = total_share * close_price * 100000000
        if market_value > 50000000000:
            continue
        if (len(stock_data) < 200):
            continue

        if(stock_id == '000672.SZ'):
            stock_data.to_csv("./data/" + stock_id +".csv", index=True, float_format='%.2f')
        if (count > 300):
            break
        count = count + 1
        df = TusharePandasData(dataname=stock_data,
                               fromdate=datetime.datetime(2020, 9, 5),
                               todate=datetime.datetime(2023, 5, 30)
                               )
        cerebro.adddata(df, name=stock_id)


    # broker设置资金、手续费
    cerebro.broker.setcash(1000000.0)
    comminfo =StockAcommissionScheme(stamp_duty=0.005, commission=0.02)
    cerebro.broker.addcommissioninfo(comminfo)
    # cerebro.broker.setcommission(commission=0.0025)
    # 设置买入设置，固定买卖数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=10000)
    cerebro.addobserver(bt.observers.TimeReturn)
    cerebro.run()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # b = Bokeh(style='bar', plot_mode='single', scheme=Tradimo())
    # cerebro.plot(b)
