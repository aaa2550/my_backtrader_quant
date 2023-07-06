#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/7/27 22:28
Desc: 新浪财经-A 股-实时行情数据和历史行情数据(包含前复权和后复权因子)
https://finance.sina.com.cn/realstock/company/sh689009/nc.shtml
"""
import datetime
import os
import pickle
import warnings
from math import floor
from typing import Dict
from abc import ABC, abstractmethod

import akshare as ak
import threading
import concurrent.futures as cf
import matplotlib
import pandas as pd
from pandas import DataFrame

import Utils
from enums import SideEnum

matplotlib.use("Agg")
warnings.filterwarnings('ignore')


def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='#', print_end='\r'):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)
    if iteration == total:
        print()


class Pair:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __repr__(self):
        return f"Person(key={self.key}, value={self.value})"


# 获取数据
class DataHandler:
    # 目录
    DIR = '~/stock'
    PERIOD = 120
    # 按日期每天存top的股票
    heap_top_mapping: Dict[str, list[Pair]] = {}
    stock_line_mapping: Dict[str, DataFrame] = {}
    # 缓存股票的csv避免重复load影响性能
    stock_csv_cache = None
    stocks = None

    def __init__(self, use_cache=True, pers_path=f'/Users/yanghailong/stock/cache'):
        self.stock_csv_cache = {}
        self.heap_top_mapping = {}
        # 是否使用缓存
        if use_cache:
            print('使用缓存数据...')
            local_heap_top_mapping = self.deserialize_data(pers_path + '/local_heap_top_mapping.pkl')
            if local_heap_top_mapping is not None:
                self.heap_top_mapping = local_heap_top_mapping
            local_stock_line_mapping = self.deserialize_data(pers_path + '/stock_line_mapping.pkl')
            if local_stock_line_mapping is not None:
                self.stock_line_mapping = local_stock_line_mapping
            print('缓存加载数据完成...')
            return

        print('重新构建数据...')
        datas = self.load_stocks()

        # for index, stock in datas.iterrows():
        #     stock = stock['代码']
        #     print(stock)
        #     data = self.load_stock(stock)
        #     # 加一些扩展的列，比如移动均线
        #     data = self.get_up_interval_or_extent(stock, data)
        #     self.stock_line_mapping[stock] = data
        #     # 计算半年内涨幅记录到每天行情
        #     self.load_top_by_day(stock, data)

        for stock in datas:
            self.load_stock_line_mapping(stock)

        self.serialize_data(self.heap_top_mapping, pers_path + '/local_heap_top_mapping.pkl')
        self.serialize_data(self.stock_line_mapping, pers_path + '/stock_line_mapping.pkl')
        print(f'load_stock_line_mapping执行结束...')

        with cf.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(self.load_top_by_day, stock, self.stock_line_mapping.get(stock)) for stock in datas]
            cf.wait(futures)
        datas['代码'].apply(lambda row: self.load_top_by_day(row, self.stock_line_mapping.get(row)))

        # for index, stock in datas.iterrows():
        #     stock = stock['代码']
        #     data = self.load_stock(stock)
        #     # 计算半年内涨幅记录到每天行情
        #     self.load_top_by_day(stock, data)

        # 半年内涨幅排序后保留top10
        for key, value in self.heap_top_mapping.items():
            sorted_values = sorted(value, key=lambda p: p.value, reverse=True)
            self.heap_top_mapping[key] = sorted_values[:10]

        print('数据构建完成...')
        # 加载到缓存
        print('数据写入缓存完成...')

    def load_stock_line_mapping(self, stock: str):
        try:
            data = self.load_stock(stock)
            data = self.get_up_interval_or_extent(stock, data)
            self.stock_line_mapping[stock] = data
        except Exception as e:
            print(repr(e))
        finally:
            print(stock)

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

    # 获取股票代码
    def load_stocks(self):
        if self.stocks is None:
            datas = pd.read_csv(f'{self.DIR}/stocks.csv')
            datas = datas[~datas['代码'].str.startswith('bj')]
            datas['代码'] = datas['代码'].str.slice(2, None)
            stocks = datas[['代码']]
            self.stocks = []
            for stock in stocks['代码']:
                if stock[:2] == 'bj' or stock[2:5] == '688':
                    continue
                self.stocks.append(stock)
        return self.stocks

    def print_heap_top_mapping(self):
        for key, value in self.heap_top_mapping.items():
            print(f'{key}:{value}')

    # 获取股票基本信息，同时加载到缓存
    def load_stock(self, stock):
        data = self.stock_csv_cache.get(stock)
        if data is not None:
            return data
        data = pd.read_csv(f'{self.DIR}/a_daily_base_english/{stock}.csv', header=0)
        self.stock_csv_cache[stock] = data

        return data

    # 比较耗时
    def get_up_interval_or_extent(self, stock, day_data):
        day_data['5ma'] = day_data['close'].rolling(window=5, min_periods=5).mean()
        day_data['20ma'] = day_data['close'].rolling(window=20, min_periods=20).mean()
        day_data['ups'] = day_data['5ma'] > day_data['20ma']
        day_data['ups'] = (day_data['ups'] != day_data['ups'].shift()) | (day_data['ups'] == False)
        day_data['ups'] = day_data['ups'].cumsum()  # 辅助列，根据识别信号，对相邻的相同行为进行分组，便于计算每组相同行为的连续发生次数
        day_data['ups'] = day_data.groupby(['ups'])['date'].rank(method='dense').astype(
            int)  # 根据行为分组，使用窗口函数对每条行为标记连续发生次数
        # for index, row in day_data.iterrows():
        #     window_size = int(row['ups'])
        #     window_data = day_data.loc[int(max(0, index - window_size + 1)):index]
        #     up_min = window_data['low'].min()
        #     up_max = window_data['high'].max()
        #     if up_min <= 0 or up_max <= 0:
        #         day_data.loc[index, 'up_percent'] = 0
        #     else:
        #         day_data.loc[index, 'up_percent'] = (up_max - up_min) / up_min
        up_min = [0]
        up_max = [0]
        def clac(row):
            window_size = int(row['ups'])
            low = row['low']
            high = row['high']
            if window_size == 1:
                up_min[0] = low
                up_max[0] = high
            elif window_size > 1:
                if low < up_min[0]:
                    up_min[0] = low
                if high > up_max[0]:
                    up_max[0] = high
            else:
                raise Exception(f"window_size={window_size},stock={stock},row={row}")

            if up_min[0] <= 0:
                row['up_percent'] = 0
                return
            row['up_percent'] = (up_max[0] - up_min[0]) / up_min[0]

        day_data.apply(lambda row: clac(row), axis=1)
        # for index, row in day_data.iterrows():
        #     window_size = int(row['ups'])
        #     low = row['low']
        #     high = row['high']
        #     if window_size == 1:
        #         up_min = low
        #         up_max = high
        #     elif window_size > 1:
        #         if low < up_min:
        #             up_min = low
        #         if high > up_max:
        #             up_max = high
        #     else:
        #         raise Exception(f"window_size={window_size},stock={stock},index={index},row={row}")
        #
        #     if up_min <= 0:
        #         row['up_percent'] = 0
        #         continue
        #     row['up_percent'] = (up_max - up_min) / up_min

        day_data.to_csv(f'~/stock/a_daily_up_percent/{stock}.csv', index=False, float_format='%.15f')
        day_data.set_index('date', inplace=True)
        return day_data

    def calculate_up_percent(self, day_data, row):
        window_size = int(row['ups'])
        ups_min = day_data.loc[int(max(0, row.name - window_size + 1)):row.name, 'low'].min()
        ups_max = day_data.loc[int(max(0, row.name - window_size + 1)):row.name, 'high'].max()
        day_data.loc[row.name, 'up_percent'] = (ups_max - ups_min) / ups_min

    def load_top_by_day(self, stock, data):
        def clac(row):
            print(row.name)
        data.apply(lambda row: clac(row), axis=1)
        # for index, day_data in data.iterrows():
        #     top_ = self.heap_top_mapping.get(index)
        #     if day_data['up_percent'] is None:
        #         print(f'stock:{stock},index:{index}')
        #     if top_ is None:
        #         top_ = []
        #         self.heap_top_mapping[index] = top_
        #     top_.append(Pair(stock, day_data['up_percent']))


# 费率接口
class CommissionInterface(ABC):
    @abstractmethod
    def calc(self, side: SideEnum, amount: float):
        pass


# A股费率
class CommissionFeeChina(CommissionInterface, ABC):
    stamp_duty: float = 0.001  # 印花税-由国家征收，按照股票成交金额的0.1%来收取，只有在你卖股票的时候才收取，而且它是单边征收的，其他费用都是双向收费。
    trading_commission: float = 0.0002  # 交易佣金-由证券公司收取，规定最高不超过股票成交金额的0.3%，不足一万按最低5元收取。
    trading_commission_limit: float = 5  # 交易佣金最低收取
    securities_regulatory_fee: float = 0.00002  # 证监管理费-由证监会收取，按股票成交金额的0.002%双向收取，买进和卖出股票时，都要收取。
    securities_transaction_handling_fee: float = 0.0000687  # 证券交易经手费-由交易所征取，A股按成交金额的0.00487%双向收取；B股，按成交额0.00487%双向收取。
    transfer_fee: float = 0.00001  # 股票过户费-由证券交易所征收，是股票成交之后更换户名所支付的一个费用，按股票成交金额的0.001%双向收取，现在上海和深圳证券交易所都要收取该费用。

    def __init__(self, stamp_duty: float = 0.001, trading_commission: float = 0.0002, transfer_fee: float = 0.00001,
                 trading_commission_limit: float = 5, securities_regulatory_fee: float = 0.00002,
                 securities_transaction_handling_fee: float = 0.0000487):
        self.stamp_duty = stamp_duty
        self.trading_commission = trading_commission
        self.transfer_fee = transfer_fee
        self.trading_commission_limit = trading_commission_limit
        self.securities_regulatory_fee = securities_regulatory_fee
        self.securities_transaction_handling_fee = securities_transaction_handling_fee

    def calc(self, side: SideEnum, amount: float):
        trading_commission = max(amount * self.trading_commission, self.trading_commission_limit)
        transfer_fee = amount * self.transfer_fee
        securities_transaction_handling_fee = amount * self.securities_transaction_handling_fee
        total_fee = trading_commission + transfer_fee + securities_transaction_handling_fee
        if side == SideEnum.SideEnum.SELL:
            total_fee = total_fee + amount * self.stamp_duty
        return total_fee


class ResultView:
    template_url: str = None
    out_url: str = None
    k_line: list[list] = None
    categoryData: list[str] = None
    vols: list[float] = None

    def __init__(self, datas: DataFrame, template_url: str = './template.html', out_url: str = './result.html'):
        self.parse(datas)
        self.template_url = template_url
        self.out_url = out_url

    def parse(self, datas: DataFrame):
        datas = datas.reindex(columns=['open', 'high', 'low', 'close', 'vlos', 'date'])
        self.categoryData = datas['date'].tolist()
        self.vols = datas['vlos'].tolist()
        datas = datas[['open', 'high', 'low', 'close']]
        self.k_line = datas.values.tolist()

    def render(self):
        content = Utils.read(self.template_url)
        category_data = self.build_category_data()
        k_line = self.build_k_line()
        vols = self.build_vols()

        content = content.replace("#{category_data}", category_data) \
            .replace("#{k_line}", k_line) \
            .replace("#{vols}", vols)
        Utils.write(self.out_url, content)

    def build_category_data(self):
        content = ''
        for value in self.categoryData:
            content.append(f"'{value}',")
        return content.removesuffix(",")

    def build_k_line(self):
        content = ''
        for sub_list in self.k_line:
            content.append(f'[{sub_list[0]},{sub_list[1]},{sub_list[2]},{sub_list[3]}],')
        return content.removesuffix(",")

    def build_vols(self):
        content = ''
        for value in self.vols:
            content.append(f"'{value}',")
        return content.removesuffix(",")


# 机器人基类
class QuantBotBase(ABC):
    config_path: str = None
    # 每个股票的K线
    stock_line_mapping: Dict[str, DataFrame] = {}
    # 初始金额
    initial_amount: float = None
    # 当前金额
    curr_amount: float = None
    # K线日历
    calendar: list[datetime] = None
    # 操作的股票列表
    stocks: list[str] = None
    # 股票持仓[股票: 股数]
    stock_position_mapping: Dict[str, int] = {}
    # 是否开启日志
    open_log: bool = True
    # 费率
    commission: CommissionInterface = None
    out_result: bool = True

    def __init__(self, stock_line_mapping: Dict[str, DataFrame], config_path: str = '~/stock',
                 initial_amount: float = 10000,
                 start_time: datetime = None, end_time: datetime = None, stocks: list[str] = None,
                 open_log: bool = True, out_result: bool = True):
        self.config_path = config_path
        self.stock_line_mapping = stock_line_mapping
        self.initial_amount = initial_amount
        self.curr_amount = initial_amount
        self.out_result = out_result
        calendar = ak.tool_trade_date_hist_sina()
        calendar['trade_date'] = pd.to_datetime(calendar['trade_date'])
        calendar = calendar[
            (calendar['trade_date'] >= start_time) & (True if end_time is None else calendar['trade_date'] <= end_time)]
        self.calendar = calendar['trade_date'].tolist()
        self.stocks = self.load_stocks(stocks)
        self.open_log = open_log
        if open_log:
            print("构造机器人完成...")

    # 只获取指定的股票列表，如果没有指定则默认取全部
    def load_stocks(self, stocks: str = None):
        if stocks is not None:
            return stocks
        datas = pd.read_csv(f'{self.config_path}/stocks.csv')
        datas = datas[~datas['代码'].str.startswith('bj')]
        datas['代码'] = datas['代码'].str.slice(2, None)
        return datas['代码'].tolist()

    # 开始运行
    def run(self):
        if self.open_log:
            print("开始执行回测...")
        # 迭代每个K线
        for cand in self.calendar:
            # 取出当前k线所有的股票并构建一个新的pd
            one_cand_data = self.get_one_cand_stocks_data(cand)
            # 走下一个k线所有的股票
            self.next_cand_day(cand, one_cand_data)
        if self.out_result:
            try:
                result_view = ResultView()
                result_view.render()
            except Exception as e:
                print(repr(e))

    # 走下一个k线所有的股票
    def next_cand_day(self, cand: datetime, one_cand_data: DataFrame):
        [self.next(cand, one_cand_stock, self.stock_line_mapping.get(one_cand_stock['code'])) for _, one_cand_stock in
         one_cand_data.iterrows()]

    @abstractmethod
    def next(self, cand: datetime, one_cand_stock, stock_datas: DataFrame = None):
        pass

    # 取出当前k线所有的股票并构建一个新的pd
    def get_one_cand_stocks_data(self, cand: datetime):
        one_day_data = pd.DataFrame()
        for stock in self.stocks:
            stock_data = self.stock_line_mapping.get(stock)
            stock_day_data = stock_data.loc[cand]
            stock_day_data['code'] = stock
            one_day_data = one_day_data.append(stock_day_data, ignore_index=True)
        return one_day_data

    # 执行买入
    def buy(self, stock: str, time: datetime, position: float = 1.0):
        # 全仓时为1，不能大于1
        if position > 1:
            position = 1
        # 获取股票k线
        stock_data = self.stock_line_mapping.get(stock)
        # 取出当前k线
        stock_cand_data = stock_data.loc[time]
        if stock_cand_data is None:
            raise ValueError(f"{stock} at {time} time failed.")

        price = stock_cand_data['close']
        # 计算当前余额一共能买的总量
        can_buy_total_quantity = floor(self.curr_amount / price)
        # A股最少买100股，并且股数要是100的整数
        buy_quantity = int(can_buy_total_quantity * position / 100) * 100
        if buy_quantity < 100:
            return
        # 买入的金额
        buy_amount = buy_quantity * price
        commission_fee = self.commission.calc(SideEnum.SideEnum.BUY, buy_amount)
        # 加手续费后需要扣除的总金额
        need_deduct_amount = buy_amount + commission_fee
        # 如果扣除手续费后金额不够的话就减掉100股
        if self.curr_amount < need_deduct_amount:
            if buy_quantity <= 100:
                return
            buy_amount = (buy_quantity - 100) * price
            commission_fee = self.commission.calc(SideEnum.SideEnum.BUY, buy_amount)
        # 余额扣除花费的金额
        self.curr_amount -= (buy_amount + commission_fee)
        # 增加持仓
        curr_quantity = self.stock_position_mapping.get(stock)
        if curr_quantity is None:
            curr_quantity = 0
        curr_quantity += buy_quantity
        self.stock_position_mapping[stock] = curr_quantity
        # 记录日志
        self.log(stock, time, price, buy_quantity, SideEnum.SideEnum.BUY)

    # 执行卖出
    def sell(self, stock: str, time: datetime, position: float = 1.0):
        # 全仓时为1，不能大于1
        if position > 1:
            position = 1
        # 如果当前没有持仓则不做任何操作
        curr_quantity = self.stock_position_mapping.get(stock)
        if curr_quantity is None:
            return
        # 获取K线
        stock_data = self.stock_line_mapping.get(stock)
        # 获取当天k线
        stock_day_data = stock_data.loc[time]
        if stock_day_data is None:
            raise ValueError(f"{stock} at {time} time failed.")
        price = stock_day_data['close']
        # 卖出数量
        sell_quantity = int(curr_quantity * position / 100) * 100
        # 卖出应得的金额金额
        sell_amount = sell_quantity * price
        # 当前余额等于卖出应得金额扣除手续费后的钱
        self.curr_amount = self.curr_amount + sell_amount - self.commission.calc(SideEnum.SideEnum.SELL, sell_amount)
        # 减掉持仓，如果卖出后没有任何持仓则del
        curr_quantity -= sell_quantity
        if curr_quantity < 0:
            raise ValueError(f"{stock} at {time} sell_quantity > curr_quantity.sell_quantity={sell_quantity}, "
                             f"curr_quantity={curr_quantity}.")
        elif curr_quantity == 0:
            del self.stock_position_mapping[stock]
        else:
            self.stock_position_mapping[stock] = curr_quantity
        # 记录日志
        self.log(stock, time, price, sell_quantity, SideEnum.SideEnum.SELL)

    # 判断是否有某只股票的持仓，默认查所有持仓
    def exist_position(self, stock: str = None):
        if stock is not None:
            return self.stock_position_mapping.get(stock) is not None
        return len(self.stock_position_mapping) > 0

    # 清空持仓
    def clean_positions(self, time: datetime):
        for stock, data in self.stock_position_mapping.items():
            self.sell(stock, time)

    def log(self, stock, time: datetime, price, quantity, side: SideEnum):
        if self.open_log is False:
            return
        print(f'[{time}]{side}:{stock},价格:{price},数量:{quantity}...')


class ContinueRisingBot(QuantBotBase):
    data_handler: DataHandler = None

    def __init__(self, config_path: str = '~/stock', initial_amount: float = 1000000,
                 start_time: datetime = datetime.datetime(2000, 1, 1), end_time: datetime = None,
                 stocks: list[str] = None,
                 open_log: bool = True):
        self.data_handler = DataHandler(use_cache=False)
        super().__init__(self.data_handler.stock_line_mapping, config_path, initial_amount, start_time, end_time,
                         stocks, open_log)

    def next(self, time: datetime, one_cand_stock, stock_datas: DataFrame = None):
        stock = one_cand_stock['code']
        top_stocks = self.data_handler.heap_top_mapping.get(time)
        hit = False
        if top_stocks is not None:
            hit = stock in [pair.key for pair in top_stocks]
        # 检查是否持仓
        if not self.exist_position(stock):  # 没有持仓
            # 不是需要操作的股票则不做任何操作
            if not hit:
                return
            # 否则买入
            self.buy(stock, time)
        elif self.exist_position(stock):  # 否则如果当前的股票在持仓列表当中
            # 取30日最高价
            days_max_price = stock_datas.loc[time - pd.DateOffset(days=30):time, 'high'].max()
            # 收盘价站最高价下降百分比
            decline_percent = (days_max_price - one_cand_stock['close']) / days_max_price
            # 如果大于15%卖出一半，如果大于20%全仓卖出
            if 0.15 < decline_percent < 0.2:
                self.sell(stock, time, 0.5)
            elif decline_percent > 0.2:
                self.sell(stock, time, 1)


def main():
    bot = ContinueRisingBot()
    bot.run()


if __name__ == '__main__':
    main()
