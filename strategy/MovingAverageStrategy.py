from __future__ import (absolute_import, division, print_function, unicode_literals)
import backtrader as bt
# 简单策略测试
class TestStrategy(bt.Strategy):
    params = (
        ('maperiod', 15),
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
        for data in self.datas:
            self.dataclose[data._name] = data.close
            self.order[data._name] = None
            self.buyprice[data._name] = None
            self.buycomm[data._name] = None
            self.sma[data._name] = bt.indicators.SimpleMovingAverage(
                data, period=self.params.maperiod)

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

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        # 简单策略，当日收盘价大于简单滑动平均值买入，当日收盘价小于简单滑动平均值卖出
        for data in self.datas:
            if self.getposition(data).size == 0:
                if self.dataclose[data._name] > self.sma[data._name]:
                    self.log('BUY CREATE, %.2f' % self.dataclose[data._name][0])
                    # self.order[data._name] = self.buy(data=data)
                    self.buy(data=data)
            else:
                if self.dataclose[data._name] < self.sma[data._name]:
                    self.log('SELL CREATE, %.2f' % self.dataclose[data._name][0])
                    # self.order[data._name] = self.sell(data=data)
                    self.sell(data=data)

    def stop(self):
        self.log('(MA Period %2d) Ending Value %.2f' %
                 (self.params.maperiod, self.broker.getvalue()), doprint=True)
        return

