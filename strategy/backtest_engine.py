import os
from datetime import datetime, timedelta
import pandas as pd
import talib as ta
from context import Context
from tushare_tool import TushareTool
from strategy_base import StrategyBase

class BacktestEngine:
    def __init__(self,is_live:bool,ma_days):
        self.ctx = Context()
        self.tushare_tool = TushareTool()
        self.backtest_result = pd.DataFrame(columns=['stock','buy_date','buy_price','profit_5','profit_10','profit_15','profit_20','fall_days','day_range','fall_rate','amount','market_value','moneyflow','strategy_id','ma_name','waitting_days','fall_days','waitting_days_before','acc_range','fall_range','rsi'])
        self.stocks_pool = pd.DataFrame(columns=['stock','begin_date','end_date','begin_price','end_price','diff','rate'])
        self.strategy_signals = pd.DataFrame(columns=['trade_date','stock','day_range','fall_rate','ma_fall_range','acc_range','waitting_days_before','fall_days','waitting_days'])
        self.stocks_pool_file = './data/stocks.csv'
        self.backtest_result_file = './data/backtest.csv'
        self.signals_file = 'data/signals.csv'
        self.target_stocks = {}
        self.is_live = is_live
        self.strategies = {}
        self.fall_days = {}
        self.ma_name = "ma" + str(ma_days)

    def add_strategy(self,id, strategy:StrategyBase):
        self.strategies[id] = strategy

    def load_k_line_from_file(self,start_date,end_date):
        result_file = "data/k_line.csv"

        result = pd.DataFrame()
        if os.path.exists(result_file):
            result = pd.read_csv(result_file)

        result.drop_duplicates(['ts_code','trade_date'],inplace=True)
        result['range'] = (result['close'] - result['open']) / result['pre_close'] * 100
        result = result[(result['trade_date'] >= int(start_date)) & (result['trade_date'] <= int(end_date))]
        return result

    def run(self,begin_date,end_date):
        stock_basic = self.tushare_tool.get_bak_basic()
        k_line = self.load_k_line_from_file(begin_date, end_date)
        moneyflow = self.tushare_tool.load_moneyflow()
        k_line = k_line.sort_values(by=['trade_date'], axis=0, ascending=True)

        backtest_date = end_date
        count = 0
        for index, row in stock_basic.iterrows():
            stock_id = row['ts_code']
            total_share = row['total_share']
            float_share = row['float_share']
            df = k_line[k_line["ts_code"] == stock_id].copy()

            if (df.empty):
                continue

            close_price = df.iloc[-1]['close']
            market_value = total_share * close_price * 100000000
            # if market_value < 3000000000 or market_value > 80000000000:
            #     continue
            #
            # if row['pe'] < 0 :
            #     continue

            self.calc_ma(df)
            count = count + 1
            for id in self.strategies:
                result = self.strategies[id].run(df, stock_id, total_share, backtest_date)
                if len(result) > 0:
                    self.strategy_signals.loc[len(self.strategy_signals)] = result
                    net_moneyflow = 0
                    stock_moneyflow = moneyflow[(moneyflow['trade_date'] == int(backtest_date)) & (moneyflow['ts_code'] == stock_id)]
                    if len(stock_moneyflow) > 0:
                        head_df = moneyflow[moneyflow['ts_code'] == stock_id]
                        head_df_10 = head_df.tail(10)
                        sum_df = head_df_10.agg(
                            {'buy_lg_amount': 'sum', 'buy_elg_amount': 'sum', 'sell_lg_amount': 'sum',
                             'sell_elg_amount': 'sum', 'net_mf_amount': 'sum'})
                        big_buy_money = sum_df['buy_lg_amount'] * 10000 + sum_df['buy_elg_amount'] * 10000
                        big_sell_money = sum_df['sell_lg_amount'] * 10000 + sum_df['sell_elg_amount'] * 10000
                        net_moneyflow = big_buy_money - big_sell_money
                        net_moneyflow = sum_df['net_mf_amount'] * 10000
                    self.calc_profit_record(stock_id, df, backtest_date, self.strategies[id].strategy_name,
                                                self.ma_name, total_share, net_moneyflow,0,0,0,0,0)

        # stock_basic = self.tushare_tool.get_bak_basic()
        # k_line = self.load_k_line_from_file(begin_date, end_date)
        # k_line = k_line.sort_values(by=['trade_date'], axis=0, ascending=True)
        #
        # backtest_date = int(end_date)
        # if len(k_line[k_line['trade_date'] == backtest_date]) == 0:
        #     return
        #
        # for index, row in stock_basic.iterrows():
        #     stock_id = row['ts_code']
        #     # stock_id = "002258.SZ"
        #     total_share = row['total_share']
        #     df = k_line[k_line["ts_code"] == stock_id].copy()
        #     if(df.empty):
        #         continue
        #     self.calc_ma(df)

        # for id in self.strategies:
        #     result = self.strategies[id].run(df, stock_id, total_share, backtest_date)
        #     if len(result) > 0:
        #         self.strategy_signals.loc[len(self.strategy_signals)] = result
        #         print(result)

        if os.path.exists(self.signals_file):
            self.strategy_signals.to_csv(self.signals_file,header=None,index=False,mode='a',float_format='%.2f')
        else:
            self.strategy_signals.to_csv(self.signals_file,index=False,float_format='%.2f')

    def run_backtest(self,begin_date,end_date):
        stock_basic = self.tushare_tool.get_bak_basic()
        k_line = self.load_k_line_from_file(begin_date, end_date)
        moneyflow = self.tushare_tool.load_moneyflow()
        k_line = k_line.sort_values(by=['trade_date'], axis=0, ascending=True)

        backtest_end_date = datetime.strptime(end_date,'%Y%m%d')
        count = 0
        for index, row in stock_basic.iterrows():
            stock_id = row['ts_code']
            total_share = row['total_share']
            float_share = row['float_share']
            df = k_line[k_line["ts_code"] == stock_id].copy()

            if(df.empty):
                continue


            close_price = df.iloc[-1]['close']
            market_value = total_share * close_price * 100000000
            # if market_value < 5000000000 or market_value > 80000000000:
            #     continue
            if market_value > 20000000000:
                continue

            if row['pe']  <= 0 or row['pe'] > 200:
                continue

            # if row['profit_yoy'] < 10:
            #     continue
            self.calc_ma(df)

            count = count + 1
            # if count < 1000 :
            #     continue
            trade_date = datetime.strptime(begin_date, '%Y%m%d') + timedelta(260)
            trade_date = datetime.strptime('20210101', '%Y%m%d')
            # trade_date = backtest_end_date
            print("start backtest stock_id=" + str(stock_id) + " begin_date = " + begin_date + " start_date=" + trade_date.strftime('%Y%m%d') + " end_date=" + end_date + " count=" + str(count))
            while trade_date <= backtest_end_date:
                backtest_date = trade_date.strftime('%Y%m%d')
                for id in self.strategies:
                    result = self.strategies[id].run(df,stock_id, total_share,backtest_date)
                    if len(result) > 0:
                        fall_days = result[-2]
                        waitting_days = result[-1]
                        waitting_days_before = result[-3]
                        acc_range = result[-4]
                        fall_range = result[-6]
                        net_moneyflow = 0
                        stock_moneyflow = moneyflow[(moneyflow['trade_date'] == int(backtest_date)) & (moneyflow['ts_code'] == stock_id)]
                        if len(stock_moneyflow) > 0:
                            row =  stock_moneyflow.iloc[0]
                            head_df = moneyflow[moneyflow['ts_code'] == stock_id]
                            head_df_10 = head_df.tail(10)
                            sum_df = head_df_10.agg({'buy_lg_amount':'sum','buy_elg_amount':'sum','sell_lg_amount':'sum','sell_elg_amount':'sum','net_mf_amount':'sum'})
                            big_buy_money = sum_df['buy_lg_amount'] * 10000 + sum_df['buy_elg_amount'] * 10000
                            big_sell_money = sum_df['sell_lg_amount'] * 10000 + sum_df['sell_elg_amount'] * 10000
                            net_moneyflow = big_buy_money - big_sell_money
                            net_moneyflow = sum_df['net_mf_amount'] * 10000
                        self.calc_profit_record(stock_id,df,backtest_date,self.strategies[id].strategy_name,self.ma_name,total_share,net_moneyflow,waitting_days,fall_days,waitting_days_before,acc_range,fall_range)
                trade_date = trade_date + timedelta(1)

    def calc_profit(self,df,trade_date,hold_days):
        profit = 0
        df_len = len(df)
        if df_len < 2:
            return profit
        cur_row = df[df['trade_date'] == trade_date].iloc[0]
        buy_price = cur_row['close']
        fall_days = 0
        max_price = 0
        for i in range(1,hold_days, 1):
            if df_len <= i:
                break
            sell_row = df.iloc[i]
            sell_price = sell_row["close"]
            pct_chg = sell_row['pct_chg']
            profit_rate = (sell_price - buy_price) / buy_price * 100
            profit = round(profit_rate * 100000 / 100, 2)
            if max_price < sell_price:
                max_price = sell_price

            fall_rate = (sell_price - max_price)/max_price * 100
            if (profit_rate < -20 and pct_chg > -9.8 ) :
                break;

        return profit

    def calc_fall_days(self,df,trade_date):
        fall_days = 0
        df = df[df['trade_date'] < int(trade_date)]
        for i in range(-1, -120, -1):
            row = df.iloc[i]
            if row[self.ma_name] > row['close']:
                fall_days += 1
            else:
                break
        return fall_days

    def calc_profit_record(self, stock_id, df, backtest_date,strategy_name,ma_name, total_share,money_flow,waitting_days,fall_days,waitting_days_before,acc_range,fall_range):
        # order_date = datetime.strptime(backtest_date,'%Y%m%d')
        trade_date = int(backtest_date)
        if len(df[df["trade_date"] == trade_date]) == 0:
            return

        tail_df = df[df["trade_date"].astype(int) >= trade_date]
        if (len(tail_df) == 0):
            return

        tail_df = tail_df.sort_values(by=['trade_date'], axis=0, ascending=True)
        cur_row = tail_df.iloc[0]

        head_df = df[df["trade_date"].astype(int) < trade_date]
        # if stock_id in self.ctx.orders:
        #     last_trade_date = str(self.ctx.orders[stock_id])
        #     if datetime.strptime(last_trade_date, '%Y%m%d') + timedelta(15) >= order_date:
        #         return

        # self.ctx.orders[stock_id] = cur_row['trade_date']

        if self.is_live:
            return

        close_price = cur_row['close']
        market_value = close_price * total_share * 100000000
        open_price = cur_row['open']
        pre_close_price = cur_row['pre_close']
        insert_date = cur_row['trade_date']
        amount = cur_row['amount'] * 1000
        ma = cur_row[self.ma_name]
        day_range = (close_price - open_price) / pre_close_price * 100
        fall_rate = round((close_price - ma) / ma * 100,2)

        # max_price = float(head_df["close"].max())
        # max_price_date = head_df[head_df['close'] > max_price - 0.000001].iloc[-1]['trade_date']
        # max_price_df = head_df[head_df['trade_date'] > max_price_date]
        # if len(max_price_df) > 30:
        #     return []
        # min_price = max_price_df['close'].min()
        # min_price_date = head_df[(head_df['trade_date'] > max_price_date) & (head_df['close'] < min_price + 0.000001)][
        #     'close'].min()

        # fall_days = (datetime.strptime(backtest_date, '%Y%m%d') -  datetime.strptime(str(max_price_date),'%Y%m%d')).days
        #hold 10 15 20 days
        profit_5 = self.calc_profit(tail_df,trade_date,5)
        profit_10 = self.calc_profit(tail_df,trade_date,10)
        profit_15 = self.calc_profit(tail_df,trade_date,15)
        profit_20 = self.calc_profit(tail_df,trade_date,100)
        rsi = cur_row['rsi']
        # fall_days = self.calc_fall_days(df,trade_date)

        self.backtest_result.loc[len(self.backtest_result)] = [stock_id, insert_date, close_price,profit_5,profit_10,profit_15,profit_20,fall_days,day_range,fall_rate,amount,market_value,money_flow,strategy_name,ma_name,waitting_days,fall_days,waitting_days_before,acc_range,fall_range,rsi]
        if os.path.exists(self.backtest_result_file):
            self.backtest_result.to_csv(self.backtest_result_file, mode='a', header=None, index=False,
                                        float_format='%.2f')
        else:
            self.backtest_result.to_csv(self.backtest_result_file, index=False, float_format='%.2f')

        self.backtest_result.drop(self.backtest_result.index, inplace=True)
        self.stocks_pool.drop(self.stocks_pool.index, inplace=True)

    def calc_ma(self,df:pd.DataFrame):
        df['ma60'] = ta.MA(df['close'], 60)
        df['ma30'] = ta.MA(df['close'], 30)
        df['ma20'] = ta.MA(df['close'], 20)
        df['ma10'] = ta.MA(df['close'], 10)
        df['ma5'] = ta.MA(df['close'], 5)

        df['ma_v_60'] = ta.MA(df['vol'], 60)
        df['ma_v_30'] = ta.MA(df['vol'], 30)
        df['ma_v_20'] = ta.MA(df['vol'], 20)
        df['ma_v_10'] = ta.MA(df['vol'], 10)
        df['ma_v_5'] = ta.MA(df['vol'], 5)
        df['rsi'] = ta.RSI(df['close'],timeperiod=6)
