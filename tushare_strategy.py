import tushare as ts
import pandas as pd
import sys
import os
import time
import threading
from datetime import datetime, timedelta


AMOUNT_BILLION = 1000000000
class StrategyBase():
    def __init__(self):
        self.min_days = 120
        self.ma_name = "ma5"
        pass

    def run(self, df, stock_id, total_share, backtest_date):
        pass

class BullStrategy(StrategyBase):
    def __init__(self):
        StrategyBase.__init__(self)

    #高位盘整
    def is_waitting(self,df,max_price_date):
        waitting_days = 0
        begin_date = 0
        up_days_5_plus = 0
        fall_days_5_plus = 0

        up_days = 0
        down_days = 0
        max_price = 0
        pre_day_price = df.iloc[-1]['close']

        #新高在过去15个交易日内,且股价大幅波动天数比较少
        for i in range(-1,-15,-1):
            sell_row = df.iloc[i]
            open_price = sell_row['open']
            close_price = sell_row['close']
            pre_close_price = sell_row['pre_close']
            begin_date = sell_row['trade_date']
            pct_chg = sell_row['pct_chg']
            if begin_date == max_price_date:
                max_price = close_price
                break
            # vol = sell_row['vol']
            day_range = (close_price - open_price) / pre_close_price * 100
            if day_range < -5 or pct_chg < -5 :
                fall_days_5_plus += 1
            elif day_range > 5 or pct_chg > 5:
                up_days_5_plus += 1
            else:
                waitting_days += 1

            if day_range > 0:
                up_days += 1
            else :
                down_days += 1

        #自高点回撤的幅度
        fall_range = -1
        if max_price > 0:
            fall_range =  (pre_day_price - max_price)/ max_price * 100

        #近15个交易日，涨幅大于5或小于-5的天数都小于三天，-5到5之间的天数大于三天，最高价在15个交易日内
        return fall_days_5_plus < 3 and up_days_5_plus < 3 and waitting_days > 3 and max_price_date == begin_date and fall_range < 0

    def run(self, df, stock_id, total_share, backtest_date):
        trade_date = int(backtest_date)
        if len(df[df["trade_date"] == trade_date]) == 0:
            return []

        if len(df) < self.min_days:
            return []

        head_df = df[df["trade_date"] < trade_date]
        if (len(head_df) < self.min_days):
            return []

        head_df = head_df.tail(self.min_days)
        tail_df = df[df["trade_date"].astype(int) >= trade_date]

        head_df = head_df.sort_values(by=['trade_date'], axis=0, ascending=True)
        tail_df = tail_df.sort_values(by=['trade_date'], axis=0, ascending=True)

        first_row = head_df.iloc[0]
        cur_row = tail_df.iloc[0]
        mid_row = head_df.iloc[int(self.min_days / 2)]
        close_price = cur_row["close"]
        open_price = cur_row["open"]
        vr = cur_row['volume_ratio']
        pre_close_price = cur_row['pre_close']
        #近段时间涨幅，时间可以 是120 150 180 个交易日，自由设置
        up_range = (cur_row['close'] - first_row['close']) / first_row['close'] * 100

        #期初 期中 期末均线价格，均线向上
        cur_ma = cur_row[self.ma_name]
        first_ma = first_row[self.ma_name]
        mid_ma = mid_row[self.ma_name]
        if cur_ma < mid_ma or mid_ma < first_ma or cur_ma < first_ma or up_range < 20:
            return []

        #采样均线，判断均线是否向上
        last_ma = head_df.iloc[-1][self.ma_name]
        grow_count = 0
        count = 0;
        for i in range(-20, -self.min_days, -20):
            row = head_df.iloc[i]
            ma = row[self.ma_name]
            if last_ma > ma:
                grow_count = grow_count + 1
            last_ma = ma
            count += 1

        if grow_count  < count * 0.8:
            return []

        max_price = float(head_df["close"].max())
        max_price_df = head_df[head_df['close'] > max_price - 0.00001];
        max_price_trade_date = int(max_price_df.iloc[0]["trade_date"])
        #自高点下来，回撤幅度大于-15
        fall_range = round((close_price - max_price) / max_price, 2) * 100
        if fall_range < -15:
            return []

        #当日阳线涨幅
        day_range = round((close_price - open_price) / pre_close_price, 2) * 100
        condition1 = self.is_waitting(head_df,max_price_trade_date) and day_range > 3 and vr > 1.2

        if condition1:
            return [stock_id]

        return []

class FallBackStrategy(StrategyBase):
    def __init__(self):
        StrategyBase.__init__(self)

    def calc_stock_score(self,stocks):
        for index, row in stocks.iterrows():
            pass

    #计算大阴线平均成交量
    def calc_sell_volume(self,df):
        sell_volume = 0
        count = 0
        for i in range(-1,-10,-1):
            sell_row = df.iloc[i]
            open_price = sell_row['open']
            close_price = sell_row['close']
            pre_close_price = sell_row['pre_close']
            vol = sell_row['vol']
            day_range = (close_price - open_price) / pre_close_price * 100
            if day_range < -5 or sell_row['pct_chg'] < -5 :
                sell_volume += vol
                count += 1

        if count > 0:
            return sell_volume / count
        else:
            return 0


    #均线上采样，判定向上趋势
    def is_up_trend(self,df):
        last_ma = df.iloc[-1][self.ma_name]
        grow_count = 0
        count = 0;

        for i in range(-20, -self.min_days, -20):
            row = df.iloc[i]
            ma = row[self.ma_name]
            if last_ma > ma:
                grow_count = grow_count + 1
            last_ma = ma
            count += 1

        return grow_count > int(count * 0.8)

    def run(self, df, stock_id, total_share, backtest_date):
        trade_date = int(backtest_date)
        if len(df[df["trade_date"] == trade_date]) == 0:
            return []

        if len(df) < self.min_days:
            return []

        head_df = df[df["trade_date"] < trade_date]
        if (len(head_df) < self.min_days):
            return []

        head_df = head_df.tail(self.min_days)
        tail_df = df[df["trade_date"].astype(int) >= trade_date]

        head_df = head_df.sort_values(by=['trade_date'], axis=0, ascending=True)
        tail_df = tail_df.sort_values(by=['trade_date'], axis=0, ascending=True)

        first_row = head_df.iloc[0]
        cur_row = tail_df.iloc[0]
        mid_row = head_df.iloc[int(self.min_days / 2)]
        close_price = cur_row["close"]
        open_price = cur_row["open"]
        vr = cur_row['volume_ratio']
        pre_close_price = cur_row['pre_close']
        vol = cur_row['vol']
        cur_day_range = (close_price - open_price) / pre_close_price * 100
        up_range = (cur_row['close'] - first_row['close']) / first_row['close'] * 100

        #起初 期中 期末价格依次向上
        cur_ma = cur_row[self.ma_name]
        first_ma = first_row[self.ma_name]
        mid_ma = mid_row[self.ma_name]
        if cur_ma < mid_ma or mid_ma < first_ma or cur_ma < first_ma or up_range < 20:
            return []

        max_price = float(head_df["close"].max())

        #自高点回撤幅度大于-30%
        fall_range = round((close_price - max_price) / max_price, 2) * 100
        if fall_range < -30:
            return []

        #周期内呈向上涨的趋势(周期可以是 120 150 180天，根据self.min)
        if not self.is_up_trend(head_df):
            return []

        # 成交金额大于3亿(涨停除外)
        # if (pct_chg < 9.8 or (pct_chg > 11 and pct_chg > 19.5)) and amount < 0.2 * AMOUNT_BILLION:
        #     return []

        # 市值大于50亿
        # market_value = float(total_share) * 100000000 * close_price
        # if (market_value < 5 * AMOUNT_BILLION):
        #     return []

        # start_day_price = head_df.iloc[0]['close']
        # diff = round(close_price - start_day_price, 2)
        # range_rate = round(diff / start_day_price * 100, 2)
        # max_price = float(head_df["close"].max())
        # fall_range = round((close_price - max_price) / max_price, 2) * 100

        # max_price_df = head_df[head_df['close'] > max_price - 0.00001];
        # max_price_trade_date = int(max_price_df.iloc[0]["trade_date"])
        # min_price = float(head_df[(head_df["trade_date"] >= max_price_trade_date) & (head_df["trade_date"] <= trade_date)]["close"].min())
        # max_fallback = round((min_price - max_price) / max_price, 2) * 100

        # 近半年涨幅大于30%且最近回撤小于20%且最大回撤小于30%
        # ma60 = cur_row['ma60']
        # fall_rate = (close_price - ma60) / ma60 * 100
        #condition = fall_rate < -5 and fall_rate > -20 and cur_day_range > 3 and vr > 1.2
        sell_vol = self.calc_sell_volume(head_df)
        #当日阳线涨幅大于3且量比大于1且成交量大于之前阴线成交量平均值
        condition = cur_day_range > 3 and vr > 1.0 and  sell_vol < vol
        if condition:
            ma60 = cur_row[self.ma_name]
            ma_fall_rate = round((close_price - ma60) / ma60 * 100, 2)
            return [backtest_date,stock_id, cur_day_range, fall_range, ma_fall_rate]

        return []

class TushareTool:
    token = 'c3f87035d1bdaf8d946f8eca7e0de867e9ef30a40dc226973f44762f'
    ts.set_token(token)
    pro = ts.pro_api()

    def __init__(self):
        pass

    def get_all_stocks(self):
        return TushareTool.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

    def get_bak_basic(self):
        basic_file= 'F:\md\price\Basic.csv'
        result = pd.DataFrame()
        if os.path.exists(basic_file):
            result = pd.read_csv(basic_file)

        if len(result) > 0:
            return result
        today = datetime.today()
        trade_date = today - timedelta(30);
        if len(result) > 0:
            trade_date =datetime.strptime(str(result['trade_date'].max()),'%Y%m%d')

        # keep 15 days
        if trade_date + timedelta(15) > today:
            return result

        trade_date = today
        while len(result) == 0:
            result = TushareTool.pro.bak_basic(trade_date=trade_date.strftime('%Y%m%d'))
            trade_date = trade_date - timedelta(1)

        result.to_csv(basic_file,  index=False,float_format='%.2f')
        return result

    def download_k_line(self,stock, start_date, end_date):
        result = ts.pro_bar(ts_code=stock, start_date=start_date, end_date=end_date, adj='qfq', ma=[5, 10, 20, 30,60],
                            factors=['tor', 'vr'])
        return result

    def download_daily_line(self,stocks):
        k_line_file = "F:\md\price\k_line.csv"
        result = pd.DataFrame()
        if os.path.exists(k_line_file):
            result = pd.read_csv(k_line_file)

        today = datetime.today()
        end_date = today.strftime('%Y%m%d')
        count = 0

        if len(result[result['trade_date'] == int(end_date)]) > 3888:
            return

        for index, row in stocks.iterrows():
            stock = row ['ts_code']
            start_date = (today - timedelta(365 * 3)).strftime('%Y%m%d')
            if len(result) == 0:
                count = count + 1
                df = self.download_k_line(stock, start_date, end_date)
                if not os.path.exists(k_line_file):
                    df.to_csv(k_line_file, index=False, float_format='%.2f')
            else:
                stock_k_line = result[result['ts_code'] == stock]
                start_date = (today - timedelta(365 * 3)).strftime('%Y%m%d')
                if (len(stock_k_line) > 0):
                    last_end_date = int(stock_k_line['trade_date'].max())
                    start_date = (datetime.strptime(str(last_end_date),'%Y%m%d') + timedelta(-90)).strftime('%Y%m%d')
                if start_date < end_date:
                    count = count + 1
                    df = self.download_k_line(stock,start_date, end_date)
                    if df is None :
                        continue

                    df = df[df['trade_date'] >str(last_end_date)]
                    df.to_csv(k_line_file, mode='a', header=None, index=False, float_format='%.2f')

            print('download k line stock=' + stock + " " + start_date + " " + end_date + " " + str(count))

            if count % 20 == 0:
                time.sleep(1)

class Context:
    def __init__(self):
        self.balance = 1000000
        self.orders = {}
        self.positions = pd.DataFrame()

class BacktestEngine:
    def __init__(self,is_live:bool):
        self.ctx = Context()
        self.tushare_tool = TushareTool()
        self.backtest_result = pd.DataFrame(columns=['stock','buy_date','buy_price','profit_5','profit_10','profit_15','profit_20','fall_days','day_range','fall_rate','strategy_id','ma_name'])
        self.stocks_pool = pd.DataFrame(columns=['stock','begin_date','end_date','begin_price','end_price','diff','rate'])
        self.strategy_signals = pd.DataFrame(columns=['trade_date','stock','day_range','fall_rate','ma_fall_range'])
        self.stocks_pool_file = 'f:/md/price/stocks.csv'
        self.backtest_result_file = 'f:/md/price/backtest.csv'
        self.signals_file = 'f:/md/price/signals.csv'
        self.target_stocks = {}
        self.is_live = is_live
        self.strategy = FallBackStrategy()
        self.strategies = {}
        self.fall_days = {}

    def add_strategy(self,id, strategy:StrategyBase):
        self.strategies[id] = strategy

    def load_k_line_from_file(self,start_date,end_date):
        result_file = "F:\md\price\k_line.csv"

        result = pd.DataFrame()
        if os.path.exists(result_file):
            result = pd.read_csv(result_file)

        result = result[(result['trade_date'] >= int(start_date)) & (result['trade_date'] <= int(end_date))]
        return result

    def run(self,begin_date,end_date):
        stock_basic = self.tushare_tool.get_bak_basic()
        k_line = self.load_k_line_from_file(begin_date, end_date)
        k_line = k_line.sort_values(by=['trade_date'], axis=0, ascending=True)

        backtest_date = int(end_date)
        if len(k_line[k_line['trade_date'] == backtest_date]) == 0:
            return

        for index, row in stock_basic.iterrows():
            stock_id = row['ts_code']
            total_share = row['total_share']
            df = k_line[k_line["ts_code"] == stock_id]
            for id in self.strategies:
                result = self.strategies[id].run(df, stock_id, total_share, backtest_date)
                if len(result) > 0:
                    self.strategy_signals.loc[len(self.strategy_signals)] = result
                    print(result)

        if os.path.exists(self.signals_file):
            self.strategy_signals.to_csv(self.signals_file,header=None,index=False,mode='a',float_format='%.2f')
        else:
            self.strategy_signals.to_csv(self.signals_file,index=False,float_format='%.2f')

    def run_backtest(self,begin_date,end_date):
        stock_basic = self.tushare_tool.get_bak_basic()
        k_line = self.load_k_line_from_file(begin_date, end_date)
        k_line = k_line.sort_values(by=['trade_date'], axis=0, ascending=True)

        backtest_end_date = datetime.strptime(end_date,'%Y%m%d')
        count = 0
        for index, row in stock_basic.iterrows():
            stock_id = row['ts_code']
            total_share = row['total_share']
            df = k_line[k_line["ts_code"] == stock_id]

            count = count + 1
            trade_date = datetime.strptime(begin_date, '%Y%m%d') + timedelta(260)
            print("start backtest stock_id=" + str(stock_id) + " begin_date = " + begin_date + " start_date=" + trade_date.strftime('%Y%m%d') + " end_date=" + end_date + " count=" + str(count))
            while trade_date <= backtest_end_date:
                backtest_date = trade_date.strftime('%Y%m%d')
                for id in self.strategies:
                    result = self.strategies[id].run(df,stock_id, total_share,backtest_date)
                    if len(result) > 0:
                        self.calc_profit_record(stock_id,df,backtest_date,id,self.strategies[id].ma_name)
                trade_date = trade_date + timedelta(1)

    def calc_profit(self,df,trade_date,hold_days):
        profit = 0
        df_len = len(df)
        if df_len < 2:
            return profit
        cur_row = df[df['trade_date'] == trade_date].iloc[0]
        buy_price = cur_row['close']
        fall_days = 0
        for i in range(1,hold_days, 1):
            if df_len <= i:
                break
            sell_row = df.iloc[i]
            sell_price = sell_row["close"]
            profit_rate = (sell_price - buy_price) / buy_price * 100
            profit = round(profit_rate * 100000 / 100, 2)
            if profit_rate < -12 and sell_row['pct_chg'] > -9.8:
                break;

            if sell_row['close'] < sell_row['ma5']:
                fall_days += 1
            else :
                fall_days = 0

            if fall_days > 4 :
                break

        return profit

    def calc_fall_days(self,df,trade_date):
        fall_days = 0
        df = df[df['trade_date'] < int(trade_date)]
        for i in range(-1, -120, -1):
            row = df.iloc[i]
            if row[self.strategy.ma_name] > row['close']:
                fall_days += 1
            else:
                break
        return fall_days

    def calc_profit_record(self, stock_id, df, backtest_date,strategy_name,ma_name):
        # order_date = datetime.strptime(backtest_date,'%Y%m%d')
        trade_date = int(backtest_date)
        if len(df[df["trade_date"] == trade_date]) == 0:
            return

        tail_df = df[df["trade_date"].astype(int) >= trade_date]
        if (len(tail_df) == 0):
            return

        tail_df = tail_df.sort_values(by=['trade_date'], axis=0, ascending=True)
        cur_row = tail_df.iloc[0]

        # if stock_id in self.ctx.orders:
        #     last_trade_date = str(self.ctx.orders[stock_id])
        #     if datetime.strptime(last_trade_date, '%Y%m%d') + timedelta(15) >= order_date:
        #         return

        # self.ctx.orders[stock_id] = cur_row['trade_date']

        if self.is_live:
            return

        close_price = cur_row['close']
        open_price = cur_row['open']
        pre_close_price = cur_row['pre_close']
        insert_date = cur_row['trade_date']
        ma60 = cur_row[self.strategy.ma_name]
        day_range = (close_price - open_price) / pre_close_price * 100
        fall_rate = round((close_price - ma60) / ma60 * 100,2)

        #hold 10 15 20 days
        profit_5 = self.calc_profit(tail_df,trade_date,5)
        profit_10 = self.calc_profit(tail_df,trade_date,10)
        profit_15 = self.calc_profit(tail_df,trade_date,15)
        profit_20 = self.calc_profit(tail_df,trade_date,100)
        fall_days = self.calc_fall_days(df,trade_date)

        self.backtest_result.loc[len(self.backtest_result)] = [stock_id, insert_date, close_price,profit_5,profit_10,profit_15,profit_20,fall_days,day_range,fall_rate,strategy_name,ma_name]
        if os.path.exists(self.backtest_result_file):
            self.backtest_result.to_csv(self.backtest_result_file, mode='a', header=None, index=False,
                                        float_format='%.2f')
        else:
            self.backtest_result.to_csv(self.backtest_result_file, index=False, float_format='%.2f')

        self.backtest_result.drop(self.backtest_result.index, inplace=True)
        self.stocks_pool.drop(self.stocks_pool.index, inplace=True)

class BacktestThread(threading.Thread):
    def __init__(self,ma_name):
        self.ma_name = ma_name
        super().__init__(name="backtest")

    def run(self) -> None:
        engine = BacktestEngine(False)
        today = datetime.today()
        end_date = today.strftime('%Y%m%d')
        start_day = (datetime.strptime(end_date, '%Y%m%d') - timedelta(1000)).strftime('%Y%m%d')

        # strategy = BullStrategy()
        strategy = FallBackStrategy()
        strategy.ma_name = self.ma_name
        engine.add_strategy('FallbackStrategy', strategy)
        # engine.add_strategy('FallBackStrategy',FallBackStrategy())

        engine.backtest_result_file = 'f:/md/price/' + self.ma_name + '.csv'
        # engine.run_backtest(start_day, end_date)
        engine.run(start_day,end_date)
        engine.ctx.orders.clear()


if __name__ == "__main__":
    engine = BacktestEngine(False)
    stocks = engine.tushare_tool.get_all_stocks()
    today = datetime.today()
    end_date = today.strftime('%Y%m%d')
    start_day = ( datetime.strptime(end_date,'%Y%m%d') - timedelta(1000)).strftime('%Y%m%d')
    engine.tushare_tool.download_daily_line(stocks)

    # engine.tushare_tool.download_daily_line(stocks)

    # engine.add_strategy('BullStrategy',BullStrategy())
    # engine.add_strategy('FallBackStrategy',FallBackStrategy())

    #ma_array = ['ma5','ma10','ma20','ma30','ma60']
    ma_array = ['ma60']
    threads = {}
    for ma_name in ma_array:
        thread = BacktestThread(ma_name)
        threads[ma_name] = thread
        thread.start()

    for thread_id in threads:
        threads[thread_id].join()

    #engine.tushare_tool.download_daily_line(stocks)
    # engine.run(start_day,end_date)
    # engine.run_backtest(start_day,end_date)