import pandas as pd
from datetime import datetime,timedelta
class TrendTool():
    def __init__(self,ma_days,min_days):
        self.ma_days = ma_days
        self.ma_name = "ma" + str(self.ma_days)
        self.min_days = min_days

    def is_up_trend(self, df):
        grow_count = 0
        count = 0;
        if len(df) < self.min_days:
            return False

        sampling_days= 10
        last_max_price = float(df.iloc[0:sampling_days][self.ma_name].max())
        for i in range(sampling_days, self.min_days, sampling_days):
            max_price = float(df.iloc[i:i+sampling_days][self.ma_name].max())
            if max_price > last_max_price:
                grow_count += 1
                last_max_price = max_price

            count += 1

        if grow_count < count *  0.9:
            return False

        return True

    def is_waitting1(self, df,max_price_date):
        # 新高在过去20个交易日内,且股价大幅波动天数比较少
        max_price_index = 0
        waitting_days = 0
        lastest_fall_days = 0
        max_price = 0;
        first_day_price = 0
        waitting_days_before_max_price = 0
        min_price = 99999
        min_price_index = 0
        rebound_index = 0
        rebound_acc_days = 0
        lastest_waitting_days = 0
        rebound_range = -1
        pre_close_price = 0
        lastest_continue_waitting_days = 0
        latest_max_price_date = 0

        for i in range(-20, 0, 1):
            sell_row = df.iloc[i]
            open_price = sell_row['open']
            close_price = sell_row['close']
            pre_close_price = sell_row['pre_close']
            trade_date = sell_row['trade_date']
            high_price = sell_row['high']
            max_range = (sell_row['high'] - sell_row['low']) / pre_close_price * 100

            pre_close_price = close_price;

            if first_day_price == 0:
                first_day_price = close_price

            if min_price > close_price and trade_date > max_price_date:
                min_price_index = i
                rebound_acc_days = 0
                min_price = close_price

            rebound_range = (close_price - min_price) / min_price * 100
            if min_price_index != 0 and rebound_range > 15 and i < -3 and i - max_price_index > 10:
                rebound_index = i

            # up_days = 0
            day_range = (close_price - open_price) / pre_close_price * 100

            if day_range> -3 and day_range< 3 and max_range < 4:
                waitting_days += 1
                lastest_waitting_days += 1
                lastest_continue_waitting_days += 1

                if trade_date < max_price_date:
                    waitting_days_before_max_price += 1
            else:
                lastest_continue_waitting_days = 0

            if day_range > 5:
                rebound_acc_days += 1

            if max_price < high_price:
                max_price = high_price
                max_price_index = i
                waitting_days = 0
                lastest_waitting_days = 0
                lastest_fall_days = 0
                latest_max_price_date = trade_date

            if day_range < -3 and i > -6:
                lastest_fall_days += 1


        # max_fall_range = (min_fall_price - max_price) / max_price * 100
        # return up_acc_days - acc_up_days_after_max_price_date < 3 and ma_fall_days < 10 and fall_acc_days > 0 and max_fall_range > -25 and waitting_days > int(max_price_index * -1 * 0.3)
        # return signal_days == 0 and fall_days > up_days and ma_fall_days < 10 and last_fall_acc_days > 0 and max_fall_range > -20
        # return  fall_days > 2 and ma_fall_days < 10 and lastest_fall_days > 0 and max_fall_range > -20 #no last fall acc days
        # return ((fall_acc_days > 0 and ma_fall_days < 10) or waitting_days > 15) and last_fall_days > 0 and max_fall_range > -20
        # return  i < -15 and ma_fall_days < 10 and (waitting_days > 13) and fall_acc_days > 0 and max_fall_range > -25  #best condition
        # return fall_acc_days < 4 and lastest_fall_days > 1 and max_price_index < -10 and ma_fall_days < 10 and (waitting_days > 10)
        # return (lastest_fall_days > 1 or lastest_fall_acc_days > 0) and max_price_index < -10 and ma_fall_days < 10 and waitting_days > 12
        # return up_acc_days - acc_up_days_after_max_price_date < 3 and fall_acc_days < 3 and (lastest_fall_days > 1) and max_price_index < -5 and ma_fall_days ==0 and int(waitting_days > max_price_index * -1 * 0.3)

        # return up_acc_days - acc_up_days_after_max_price_date < 3 and fall_acc_days < 3 and lastest_fall_days > 0 and max_price_index < -8 and ma_fall_days < 10 and waitting_days > int(max_price_index * -1 * 0.3)
        #condition = up_acc_days - acc_up_days_after_max_price_date < 4 and lastest_fall_days > 0 and max_price_index < -6 and ma_fall_days < 10 and waitting_days > int(max_price_index * -1 * 0.3) and max_price_date == max_price_trade_date
        # condition = up_acc_days - acc_up_days_after_max_price_date < 4 and lastest_fall_days > 0 and max_price_index < -6 and ma_fall_days < 10 and max_price_date == max_price_trade_date
        date1 = datetime.strptime(str(max_price_date), '%Y%m%d')
        date2 = datetime.strptime(str(latest_max_price_date), '%Y%m%d')
        diff = date2 - date1
        condition1 = diff < timedelta(2) and diff > timedelta(-2)

        acc_range = (max_price - first_day_price) / first_day_price * 100;
        rebound_range = (pre_close_price - min_price) / min_price * 100
        condition = (lastest_waitting_days >  int(0.6 * max_price_index)) and rebound_acc_days < 2 and rebound_range < 10 and rebound_index == 0 and max_price_index < -5 and max_price_index > -15
        condition = condition1 and lastest_fall_days < 3 and (lastest_waitting_days >  int(0.6 * max_price_index)) and rebound_acc_days < 2 and rebound_range < 10 and rebound_index == 0 and max_price_index > -15
        return [condition,lastest_continue_waitting_days,waitting_days_before_max_price,waitting_days,max_price_index]
    # 计算大阴线平均成交量
    def get_max_vol_line(self, df):
        max_vol = 0
        for i in range(-1, -30, -1):
            sell_row = df.iloc[i]
            open_price = sell_row['open']
            close_price = sell_row['close']
            pre_close_price = sell_row['pre_close']
            vol = sell_row['vol']
            pct_chg = sell_row['pct_chg']
            trade_date = sell_row['trade_date']
            day_range = (close_price - open_price) / pre_close_price * 100

            if max_vol < vol:
                max_vol = vol

        return [max_vol,pct_chg,day_range,trade_date]

        # 计算大阴线平均成交量
    def is_greater_buy_power(self, df):
        sell_volume = 0
        bid_volume = 0
        max_sell_vol = 0
        max_bid_vol = 0
        for i in range(-1, -15, -1):
            sell_row = df.iloc[i]
            open_price = sell_row['open']
            close_price = sell_row['close']
            pre_close_price = sell_row['pre_close']
            vol = sell_row['vol']
            day_range = (close_price - open_price) / pre_close_price * 100

            if day_range < -2:
                sell_volume += vol
                if max_sell_vol < vol:
                    max_sell_vol = vol
            elif day_range > 2:
                bid_volume += vol
                if max_bid_vol < vol:
                    max_bid_vol = vol

        return max_bid_vol == 0 or (max_bid_vol > max_sell_vol*1.1)
