#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/7/27 22:28
Desc: 新浪财经-A 股-实时行情数据和历史行情数据(包含前复权和后复权因子)
https://finance.sina.com.cn/realstock/company/sh689009/nc.shtml
"""
import os
import datetime
from typing import Dict

import backtrader as bt
import akshare as ak
import warnings
import pickle
import matplotlib
import pandas as pd

matplotlib.use("Agg")
warnings.filterwarnings('ignore')

class Pair:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __repr__(self):
        return f"Person(key={self.key}, value={self.value})"

class DataHandler:

    DIR = '~/stock'
    PERIOD = 120
    heap_top_mapping: Dict[str, list[Pair]] = None
    stock_csv_cache = None
    stocks = None

    def __init__(self, use_pers=True, pers_path=f'/Users/yanghailong/stock/pers.pkl'):

        self.stock_csv_cache = {}
        self.heap_top_mapping = {}

        if use_pers:
            local_heap_top_mapping = self.deserialize_data(pers_path)
            if local_heap_top_mapping is not None:
                self.heap_top_mapping = local_heap_top_mapping
                return

        datas = self.load_stocks()
        for index, stock in datas.iterrows():
            stock = stock['代码']
            data = self.load_stock(stock)
            data = self.get_up_interval_or_extent(data)
            self.load_top_by_day(stock, data)

        for key, value in self.heap_top_mapping.items():
            sorted_values = sorted(value, key=lambda p: p.value, reverse=True)
            self.heap_top_mapping[key] = sorted_values[:10]
        if use_pers:
            self.serialize_data(self.heap_top_mapping, pers_path)


    def serialize_data(self, data, file_path):
        with open(file_path, 'wb') as file:
            pickle.dump(data, file)

    # 从本地文件加载数据并反序列化为字典
    def deserialize_data(self, file_path):
        if not os.path.exists(file_path):
            return None
        with open(file_path, 'rb') as file:
            data = pickle.load(file)
        return data

    def load_stocks(self):
        if self.stocks is None:
            datas = pd.read_csv(f'{self.DIR}/stocks.csv')
            datas = datas[~datas['代码'].str.startswith('bj')]
            datas['代码'] = datas['代码'].str.slice(2, None)


            self.stocks = datas[['代码']]
        return self.stocks


    def print_heap_top_mapping(self):
        for key, value in self.heap_top_mapping.items():
            print(f'{key}:{value}')

    def load_stock(self, stock):
        data = self.stock_csv_cache.get(stock)
        if data is not None:
            return data
        data = pd.read_csv(f'{self.DIR}/a_daily_2/{stock}.csv', header=0)
        self.stock_csv_cache[stock] = data

        return data

    def get_up_interval_or_extent(self, day_data):
        day_data['5ma'] = day_data['close'].rolling(window=5, min_periods=5).mean()
        day_data['20ma'] = day_data['close'].rolling(window=20, min_periods=20).mean()
        day_data['up'] = day_data['5ma'] > day_data['20ma']
        day_data['up'] = day_data['up'] != True  # 识别信号，判断行为是否发生了改变
        day_data['up'] = day_data['up'].cumsum()  # 辅助列，根据识别信号，对相邻的相同行为进行分组，便于计算每组相同行为的连续发生次数
        day_data['up'] = day_data.groupby(['up'])['date'].rank(method='dense').astype(int)  # 根据行为分组，使用窗口函数对每条行为标记连续发生次数

        # print(day_data['up'].apply(lambda x: day_data['high'].rolling(window=x, min_periods=x).max()))
        for index, row in day_data.iterrows():
            window_size = int(row['up'])
            up_min = day_data.loc[int(max(0, index - window_size + 1)):index, 'low'].min()
            up_max = day_data.loc[int(max(0, index - window_size + 1)):index, 'high'].max()
            day_data.loc[index, 'up_increase'] = (up_max - up_min) / up_min
        day_data.set_index('date', inplace=True)
        return day_data

    def load_top_by_day(self, stock, data):
        for index, day_data in data.iterrows():
            top_ = self.heap_top_mapping.get(index)
            if top_ is None:
                top_ = []
                self.heap_top_mapping[index] = top_
            top_.append(Pair(stock, day_data['up_increase']))

class MyStrategy(bt.Strategy):

    data_handler: DataHandler = None
    # 定义参数
    params = dict(data_handler=None)  # 数据帧参数

    def __init__(self):
        """
        初始化函数
        """
        print("已经开始构建策略...")
        self.data_handler = self.params.data_handler
        self.order = None
        self.buy_price = None
        self.buy_comm = None

    def next(self):
        if self.order:  # 检查是否有指令等待执行,
            return

        top_stocks = self.params.data_handler.heap_top_mapping.get(self.data0.datetime.datetime(0))
        hit = False
        if top_stocks is not None:
            hit = self.data._name not in [pair.key for pair in top_stocks]
        # 检查是否持仓
        if not self.position:  # 没有持仓
            if not hit:
                return
            self.order = self.buy(size=100)  # 执行买入
        else:
            if not hit or self.data.close[0] < self.data.low.get(size=10, ago=0):
                self.order = self.sell(size=100)  # 执行卖出

    def log(self, txt, dt=None, do_print=False):
        """
        Logging function fot this strategy
        """
        pass

    def notify_order(self, order):
        """
        记录交易执行情况
        """
        # 如果 order 为 submitted/accepted,返回空
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    f"买入:\n价格:{order.executed.price},\
                成本:{order.executed.value},\
                手续费:{order.executed.comm}"
                )
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(
                    f"卖出:\n价格：{order.executed.price},\
                成本: {order.executed.value},\
                手续费{order.executed.comm}"
                )
            self.bar_executed = len(self)

            # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("交易失败")
        self.order = None

    def notify_trade(self, trade):
        """
        记录交易收益情况
        """
        if not trade.isclosed:
            return
        self.log(f"策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}")

    def stop(self):
        """
        回测结束后输出结果
        """
        self.log("期末总资金 %.2f" % (self.broker.getvalue()), do_print=True)


def main(start_cash=1000000, commission_fee=0.001, start_time=datetime.datetime(2002, 4, 1), end_time=datetime.datetime(2015, 12, 31)):
    print(f'加载数据...')
    data_handler = DataHandler()
    print(f'加载数据完毕，构造策略数据...')
    cerebro = bt.Cerebro()  # 创建主控制器
    trace_days = ak.tool_trade_date_hist_sina()
    trace_days = trace_days.rename(columns={'trade_date':'date'})
    datas = data_handler.load_stocks()
    for index, stock in datas.iterrows():
        stock = stock['代码']
        stock_data = data_handler.load_stock(stock)
        full_stock_data = pd.merge(left=trace_days, right=stock_data, on='date', how='left')
        #full_stock_data.index = pd.to_datetime(full_stock_data['date'], format='%Y-%m-%d')
        full_stock_data.index = pd.to_datetime(full_stock_data['date'])
        data = bt.feeds.PandasData(dataname=full_stock_data,
                                   open=1,  # 开盘价所在列
                                   high=3,  # 最高价所在列
                                   low=4,  # 最低价所在列
                                   close=2,  # 收盘价价所在列
                                   volume=5,
                                   fromdate=start_time,  # 起始日2002, 4, 1
                                   todate=end_time,  # 结束日 2015, 12, 31
                                   )
        cerebro.adddata(data, name=stock)
        break

    cerebro.addstrategy(MyStrategy, data_handler=data_handler)
    cerebro.broker.setcash(start_cash)  # 设置初始资本为 100000
    cerebro.broker.setcommission(commission=commission_fee)  # 设置交易手续费为 0.2%
    print(f'构造策略数据完毕，回测数据...')
    cerebro.run()
    port_value = cerebro.broker.getvalue()  # 获取回测结束后的总资金
    pnl = port_value - start_cash  # 盈亏统计

    print(f"总资金: {round(port_value, 2)}")
    print(f"净收益: {round(pnl, 2)}")

    cerebro.plot(style='candlestick')  # 画图

if __name__ == '__main__':
    main(commission_fee=0.001)

