#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/7/27 22:28
Desc: 新浪财经-A 股-实时行情数据和历史行情数据(包含前复权和后复权因子)
https://finance.sina.com.cn/realstock/company/sh689009/nc.shtml
"""
import os
from datetime import datetime

import backtrader as bt
import backtrader.indicators as btind  # 导入策略分析模块
import matplotlib
# 导入库
import pandas as pd

matplotlib.use("Agg")


class DailyLimitStrategy(bt.Strategy):

    def __init__(self):
        self.ccc = self.datas[-1].close * 1.1
        print(dir(self.ccc))
        self.signal = self.datas[0].close == round(self.datas[-1].close * 1.1) \
                      and self.datas[-1].close == round(self.datas[-2].close * 1.1) \
                      and self.datas[0].volume > self.datas[-2].volume * 1.4 \
                      and self.datas[0].volume > self.datas[-1].volume * 1.2

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def notify_cashvalue(self, cash, value):
        self.log('Cash %s Value %s' % (cash, value))

    def notify_order(self, order):
        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

    def notify_trade(self, trade):
        pass

    def next(self):
        if self.order:
            self.cancel(self.order)
        # Check if we are in the market
        if not self.getposition(self.datas[0]):
            # self.data.close是表示收盘价
            # 收盘价大于histo，买入
            if self.signal:
                self.log('BUY CREATE,{}'.format(self.dataclose[0]))
                self.order = self.buy(self.datas[0])

        else:
            # 收盘价小于等于histo，卖出
            if not self.signal:
                self.log('BUY CREATE,{}'.format(self.dataclose[0]))
                self.order = self.sell(self.datas[0])


def date_parser(s):
    return datetime.strptime(s, '%Y/%m/%d')

def get_data(code, start='2022-01-04', end='2022-01-07'):
    df = pd.read_csv(f'~/{code}.csv')
    df.rename(columns={'date': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'},
              inplace=True)
    df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    df['Date'] = pd.to_datetime(df['Date'], format='%Y/%m/%d')
    df = df.set_index('Date').sort_index(ascending=True)
    df['Volume'] = 0
    df['OpenInterest'] = 0
    return df[start: end]

def bt3():
    start = datetime(2000, 5, 24)
    end = datetime(2022, 7, 20)
    k_data = get_data('sz000014', start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    data = bt.feeds.PandasData(dataname=k_data)
    return data

cerebro = bt.Cerebro()

cerebro.adddata(bt3())

cerebro.addstrategy(DailyLimitStrategy)
# 设置金额，默认是200000
cerebro.broker.set_cash(200000)
cerebro.run()
cerebro.plot(volume=False)
print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

