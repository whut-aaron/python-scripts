from strategy_base import StrategyBase
from trend_tool import TrendTool
class BullStrategy(StrategyBase):
    def __init__(self,ma_days,min_days):
        StrategyBase.__init__(self,'BullStrategy',ma_days,min_days)
        self.trend_tool = TrendTool(self.ma_days, self.min_days)
    def run(self, df, stock_id, total_share, backtest_date):
        trade_date = int(backtest_date)
        if len(df[df["trade_date"] == trade_date]) == 0:
            return []

        # df[self.ma_name] = ta.MA(df['close'],60)
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
        pre_row = head_df.iloc[-1]
        mid_row = head_df.iloc[int(self.min_days / 2)]
        close_price = cur_row["close"]
        open_price = cur_row["open"]
        pre_close_price = cur_row['pre_close']
        amount = cur_row[self.ma_name] * cur_row["ma_v_" + str(self.ma_days)]
        cur_day_range = (close_price - open_price) / pre_close_price * 100
        vol_rate = cur_row['volume_ratio']
        vol5 = cur_row['ma_v_5']
        vol10 = cur_row['ma_v_10']
        # up_range = (cur_row['close'] - first_row['close']) / first_row['close'] * 100

        #起初 期中 期末价格依次向上
        cur_ma = cur_row[self.ma_name]
        first_ma = first_row[self.ma_name]
        mid_ma = mid_row[self.ma_name]
        if cur_ma < mid_ma or mid_ma < first_ma or cur_ma < first_ma:
            return []

        max_price = float(head_df["high"].max())
        max_price_date = head_df[head_df['high'] > max_price - 0.000001].iloc[-1]['trade_date']
        max_price_df =head_df[head_df['trade_date']> max_price_date]
        if len(max_price_df) > 30:
            return []
        min_price = max_price_df['close'].min()
        max_price_row = head_df[head_df['trade_date'] >= max_price_date].iloc[0]
        max_price_vol = max_price_row['vol']

        #自高点回撤幅度大于-30%
        max_fall_range = round((min_price - max_price) / max_price, 2) * 100
        if max_fall_range < -25 :
            return []

        fall_range = round((close_price - max_price) / max_price, 2) * 100
        if fall_range < -20:
            return []

        #周期内呈向上涨的趋势(周期可以是 120 150 180天，根据self.min)
        if not self.trend_tool.is_up_trend(head_df):
            return []

        max_vol_line = self.trend_tool.get_max_vol_line(head_df)
        max_vol = max_vol_line[0]
        max_vol_date = max_vol_line[3]
        if(max_vol > max_price_vol * 1.5 and max_price_date < max_vol_date):
            return []

        if close_price < max_vol_line[1]:
            return []
        #当日阳线涨幅大于3且量比大于1且成交量大于之前阴线成交量平均值
        # condition = self.trend_tool.is_greater_buy_power(head_df) and self.trend_tool.is_waitting1(head_df) and vol > sell_vol and cur_day_range > -3 and tor > 2 and amount * 1000 > 300000000
        ma = cur_row[self.ma_name]
        ma_range = (close_price - ma) / ma  * 100
        pre_range = (pre_row['close'] - pre_row['open'])/pre_row['pre_close'] * 100
        waitting_result = self.trend_tool.is_waitting1(head_df,max_price_date)
        is_waitting = waitting_result and waitting_result[0]
        # condition = ma_range > -10 and is_waitting and cur_day_range < -4 and pre_range < -4 and amount*100 > 300000000
        condition = pre_range < 0 and is_waitting and cur_day_range > 6 and amount*100 > 300000000 and vol5 < vol10
        # condition = ma_range > -10 and is_waitting and cur_day_range >5 and amount*100 > 300000000 and vol > ma5_vol
        if condition:
            fall_days = waitting_result[-1]
            waitting_days = waitting_result[-2]
            waitting_days_before = waitting_result[-3]
            acc_range= waitting_result[-4]
            return [backtest_date,stock_id, cur_day_range, fall_range,ma_range,acc_range,waitting_days_before,fall_days,waitting_days]

        return []

