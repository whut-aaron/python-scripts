from strategy_base import StrategyBase
from trend_tool import TrendTool
class ReboundStrategy(StrategyBase):
    def __init__(self,ma_days,min_days):
        StrategyBase.__init__(self,'ReboundStrategy',ma_days,min_days)
        self.trend_tool = TrendTool(self.ma_days, self.min_days)

    def little_vol_trend(self,df):
        tail_df = df.tail(10)
        vol1= tail_df['vol'].sum()

        tail_df = df.tail(20).head(10)
        vol2= tail_df['vol'].sum()
        return vol1 < vol2 * 0.7

    def is_bull_trend(self,df):
        tail_df = df.tail(30)
        max_price = tail_df['close'].max()
        max_price_trade_date=tail_df[tail_df['close'] > max_price - 0.001].iloc[0]['trade_date']

        fall_back_len = len(tail_df[tail_df['trade_date'] > max_price_trade_date])
        return fall_back_len < 20 and fall_back_len > 3


    def is_up_trend(self, df):
        check_days = 90
        grow_count = 0
        count = 0;
        if len(df) < check_days:
            return False

        sampling_days= 10
        last_max_price = float(df.iloc[0:sampling_days][self.ma_name].max())
        for i in range(sampling_days, self.min_days, sampling_days):
            max_price = float(df.iloc[i:i+sampling_days][self.ma_name].max())
            if max_price > last_max_price:
                grow_count += 1
                last_max_price = max_price

            count += 1

        if grow_count < count *  0.5:
            return False

        return True


    def is_signal_trigged(self,df):
        tail_df = df.tail(10)
        total_pct_chg = tail_df['pct_chg'].sum()
        # min_pct_chg = tail_df['pct_chg'].min()

        len1 = len(tail_df[(tail_df['range'] > -1.5) & (tail_df['range'] < 1.5)])
        # return len1 > 3 and min_pct_chg < -3 and total_pct_chg < 5  and total_pct_chg > -20
        return len1 > 3 and total_pct_chg > -10

    def is_repeat_signal(self,df):
        tail_df = df.tail(5)
        if len(tail_df[tail_df['range'] > 9]) > 0:
            return True
        return False

    def is_waitting(self, df):
        check_days = 20
        tail_df = df.tail(check_days)
        condition1 = len(tail_df[(tail_df['pct_chg'] < 2) & (tail_df['pct_chg'] > -2)]) >= int((check_days) * 0.7)
        return  condition1

    def is_break_and_back(self,df):
        check_days = 10
        tail_df = df.tail(check_days)
        break_days = len(tail_df[tail_df['range'] > 6 | (tail_df['pct_chg'] > 6) ])
        if break_days > 1 :
            return False
        for i in range(0,check_days,1):
            row = tail_df.iloc[i]
            up_range = row['range']
            if up_range > 6 and i < 5:
                return False
        return True

    def is_rebound_trend(self,df):
        ma_name = "ma20"
        check_days = int(self.min_days / 3)
        step = 10
        last_ma = df.iloc[0][ma_name]
        last_up_count = 0
        for i in range(step,check_days, step):
            row = df.iloc[i]
            ma = row[ma_name]
            if ma > last_ma:
                last_up_count += 1
            last_ma = ma

        return last_up_count >= int(check_days / step * 0.5)

    def is_horizontal_price_movement(self,df):
        check_days = 60
        tail_df = df.tail(check_days)
        max_price = tail_df['close'].max()
        min_price = tail_df['close'].min()
        range = (max_price - min_price) / max_price * 100
        return range < 25

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

        cur_row = tail_df.iloc[0]
        pre_row = head_df.iloc[-1]
        close_price = cur_row["close"]
        open_price = cur_row["open"]
        pre_close_price = cur_row['pre_close']
        pct_chg = cur_row['pct_chg']
        range = cur_row['range']
        cur_day_range = (close_price - open_price) / pre_close_price * 100
        ma = cur_row[self.ma_name]
        ma_range = (close_price - ma) / ma  * 100
        pre_pct_chg = pre_row['pct_chg']

        if pct_chg < 5 or cur_day_range < 3:
            return []

        vol = cur_row['vol']
        money = vol * close_price * 100;

        if money < 200000000:
            return []

        vol_5 = cur_row['ma_v_5']
        if vol < vol_5 :
            return []

        ma60 = cur_row['ma60'];
        ma30 = cur_row['ma30'];
        if ma30 < ma60 :
            return []

        # if close_price < ma60:
        #     return []
        # if not self.is_up_trend(head_df):
        #     return []

        if not self.is_break_and_back(head_df):
            return []

        # if not self.is_waitting(head_df):
        #     return []

        # if not self.is_bull_trend(head_df):
        #     return []

        # if not self.little_vol_trend(head_df):
        #     return []

        # if not self.is_horizontal_price_movement(head_df):
        #     return []

        if not self.is_rebound_trend(head_df):
            return []

        # if self.is_repeat_signal(head_df):
        #     return []

        # condition = self.is_signal_trigged(head_df)

        fall_days = 0
        waitting_days = 0
        waitting_days_before = 0
        acc_range = 0
        return [backtest_date,stock_id, cur_day_range, 0,ma_range,acc_range,waitting_days_before,fall_days,waitting_days]