from __future__ import (absolute_import, division, print_function, unicode_literals)
import backtrader as bt
# 简单策略测试
class TrendBreakStrategy(bt.Strategy):
    params = (
        ('maperiod', 20),
        ('maperiod10', 10),
        ('maperiod5', 5),
        ('printlog', True),
    )

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = dict()
        self.order = dict()
        self.buyprice = dict()
        self.buycomm = dict()
        self.sma = dict()
        self.sma10 = dict()
        self.sma5 = dict()
        for data in self.datas:
            self.dataclose[data._name] = data.close
            self.order[data._name] = None
            self.buyprice[data._name] = None
            self.buycomm[data._name] = None
            self.sma[data._name] = bt.indicators.SimpleMovingAverage(
                data, period=self.params.maperiod)

            self.sma10[data._name] = bt.indicators.SimpleMovingAverage(
                data, period=self.params.maperiod10)

            self.sma5[data._name] = bt.indicators.SimpleMovingAverage(
                data, period=self.params.maperiod5)
            print(data._name)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Stock: %s, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.data._name,
                     order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice[order.data._name] = order.executed.price
                self.buycomm[order.data._name] = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Stock: %s, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.data._name,
                          order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
            return

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, Stock %s ,GROSS %.2f, NET %.2f' %
                 (trade.data._name, trade.pnl, trade.pnlcomm))

    def next(self):
        # 简单策略，当日收盘价大于简单滑动平均值买入，当日收盘价小于简单滑动平均值卖出
        for data in self.datas:
            if self.getposition(data).size == 0:
                if(self.is_trend_break(data) and data.pct_chg[0] > 9.90):
                    # self.log('BUY CREATE,Stock : %s Close %.2f pctchg %.2f' % (data._name,self.dataclose[data._name][0],data.pct_chg[0]))
                    self.order[data._name] = self.buy(data=data)
            else:
                if(data.close < self.sma[data._name]):
                    # self.log('SELL CREATE,Stock : %s Close %.2f pct_chg %.2f' % (data._name,self.dataclose[data._name][0],data.pct_chg[0]))
                    self.order[data._name] = self.sell(data=data)

    def stop(self):
        self.log('(MA Period %2d) Ending Value %.2f' %
                 (self.params.maperiod, self.broker.getvalue()), doprint=True)
        return

    def is_trend_break(self,data):
        down_count = 0
        step = 3
        total_days = 90
        total_count = total_days / step
        for i in range(-step,-total_days,-step):
            if(self.sma[data._name][i + step] < self.sma[data._name][i]):
                down_count = down_count + 1

        if down_count > total_count * 0.5:
            return False;

        ly_count = 0
        for i in range(-1, -20, -1):
            change_rate = data.pct_chg[i]
            if(change_rate  < 5 and change_rate > -5):
                ly_count = ly_count + 1

        return ly_count > 15