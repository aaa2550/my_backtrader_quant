#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/7/27 22:28
Desc: 新浪财经-A 股-实时行情数据和历史行情数据(包含前复权和后复权因子)
https://finance.sina.com.cn/realstock/company/sh689009/nc.shtml
"""
# 导入库
import matplotlib as matplotlib
import numpy as np
import pandas as pd
from statsmodels import regression
from sklearn import datasets
from datetime import datetime
import akshare as ak
import tushare as ts
import os, sys
import backtrader as bt
import statsmodels.api as sm
import backtrader.indicators as btind # 导入策略分析模块
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import math


class EmptyStrategy(bt.Strategy):
    def log(self):
        pass

    def __init__(self):
        pass

    def log(self, txt, dt=None):
        pass

    def notify_cashvalue(self, cash, value):
        pass

    def notify_order(self, order):
        pass

    def notify_trade(self, trade):
        pass

    def next(self):
        pass


class MACDStrategy(bt.Strategy):

    params = (
        ('period', 15),
        ('printdata', True),
        ('printops', True),
    )

    def __init__(self):
        print(self.data)
        sma1 = btind.SimpleMovingAverage(self.data)

    #        ema1 = btind.ExponentialMovingAverage()

    #         close_over_sma = self.data.close > sma1
    #         close_over_ema = self.data.close > ema1
    #         sma_ema_diff = sma1 - ema1

    #         buy_sig = bt.And(close_over_sma, close_over_em

    def next(self):
        pass
#         if buy_sig:
#             self.buy()

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

modpath = '/Users/yanghailong/PycharmProjects/backtrader/tests'
dataspath = '../datas'
datafiles = [
    '2006-day-001.txt',
    '2006-week-001.txt',
]

FROMDATE = datetime(2006, 1, 1)
TODATE = datetime(2006, 12, 31)
DATAFEED = bt.feeds.BacktraderCSVData

def getdata(index, fromdate=FROMDATE, todate=TODATE):

    datapath = os.path.join(modpath, dataspath, datafiles[index])
    data = DATAFEED(
        dataname=datapath,
        fromdate=fromdate,
        todate=todate)

    return data

def bt3():
    start = datetime(2022, 5, 24)
    end = datetime(2022, 7, 20)
    k_data = get_data('sz000014', start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    datapath = os.path.join(modpath, dataspath, '2006-day-001.txt')
    dataframe = pd.read_csv(datapath,
                                parse_dates=True,
                                index_col=0)
    data = bt.feeds.PandasData(dataname=k_data)
    print(k_data)
    return data

cerebro = bt.Cerebro()

cerebro.adddata(bt3())

cerebro.addstrategy(MACDStrategy)
# 设置金额，默认是200000
cerebro.broker.set_cash(200000)
cerebro.run()
cerebro.plot(volume=False)
print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

