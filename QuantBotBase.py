from abc import ABC, abstractmethod
from datetime import datetime
from math import floor
from typing import Dict
import akshare as ak
import pandas as pd
from pandas import DataFrame

import CommissionInterface
from CommissionFeeChina import CommissionFeeChina
from Config import pers_path
from ResultView import ResultView
import SideEnum
from Utils import deserialize_data, serialize_data


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
    one_cand_stocks_data_cache: Dict[datetime, DataFrame] = {}
    out_result: bool = True
    use_cache: bool = True

    def __init__(self, stock_line_mapping: Dict[str, DataFrame], config_path: str = '~/stock',
                 initial_amount: float = 10000,
                 start_time: datetime = None, end_time: datetime = None, stocks: list[str] = None,
                 open_log: bool = True, out_result: bool = True, commission: CommissionInterface = CommissionFeeChina(),
                 use_cache: bool = True):
        self.config_path = config_path
        self.stock_line_mapping = stock_line_mapping
        self.initial_amount = initial_amount
        self.curr_amount = initial_amount
        self.out_result = out_result
        self.commission = commission
        self.use_cache = use_cache
        calendar = ak.tool_trade_date_hist_sina()
        calendar['trade_date'] = pd.to_datetime(calendar['trade_date'])
        calendar = calendar[
            (calendar['trade_date'] >= start_time) & (True if end_time is None else calendar['trade_date'] <= end_time)]
        calendar['trade_date'] = pd.to_datetime(calendar['trade_date'])
        calendar['trade_date'] = calendar[calendar['trade_date'] <= pd.to_datetime('2023-06-21')]
        self.calendar = calendar['trade_date'].tolist()
        self.stocks = self.load_stocks(stocks)
        self.open_log = open_log
        self.load_one_cand_stocks_data()
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

    def load_one_cand_stocks_data(self):
        if self.use_cache:
            print(f"缓存加载one_cand_stocks_data数据...")
            self.one_cand_stocks_data_cache = deserialize_data(pers_path + '/one_cand_stocks_data_cache.pkl')
            return

        for cand in self.calendar:
            # 取出当前k线所有的股票并构建一个新的pd
            print(f"加载one_cand_stocks_data数据[{format(cand)}]")
            try:
                self.one_cand_stocks_data_cache[cand] = self.get_one_cand_stocks_data(cand)
            except Exception as e:
                print(repr(e))

        serialize_data(self.one_cand_stocks_data_cache, pers_path + '/one_cand_stocks_data_cache.pkl')

    # 开始运行
    def run(self):
        if self.open_log:
            print("开始执行回测...")
        # 迭代每个K线
        for cand in self.calendar:
            # 走下一个k线所有的股票
            self.next_cand_day(cand, self.one_cand_stocks_data_cache.get(cand))
        if self.out_result:
            try:
                result_view = ResultView()
                result_view.render()
            except Exception as e:
                print(repr(e))

    # 走下一个k线所有的股票
    def next_cand_day(self, cand: datetime, one_cand_data: DataFrame):
        for row in one_cand_data.itertuples():
            index = row.Index
            self.next(cand, row, index, self.stock_line_mapping.get(index))

    # cand  k线日历
    # one_cand_stock 行数据
    # stock 股票代码
    # stock_datas 当前股票所有数据
    @abstractmethod
    def next(self, cand: datetime, row, stock: str, stock_datas: DataFrame = None):
        pass

    # 取出当前k线所有的股票并构建一个新的pd
    def get_one_cand_stocks_data(self, cand: datetime):
        rows_data = []

        for stock in self.stocks:
            stock_data = self.stock_line_mapping.get(stock)

            if cand in stock_data.index:
                stock_day_data = stock_data.loc[cand].copy()
                stock_day_data['code'] = stock
                rows_data.append(stock_day_data)

        one_day_data = pd.concat(rows_data, axis=1).T
        one_day_data.set_index('code', inplace=True)
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
        if price <= 2:
            return
        # 计算当前余额一共能买的总量
        can_buy_total_quantity = floor(self.curr_amount / price)
        # A股最少买100股，并且股数要是100的整数
        buy_quantity = int(can_buy_total_quantity * position // 100) * 100
        if buy_quantity < 100:
            return
        # 买入的金额
        buy_amount = buy_quantity * price
        commission_fee = self.commission.calc(side=SideEnum.SideEnum.BUY, amount=buy_amount)
        # 加手续费后需要扣除的总金额
        need_deduct_amount = buy_amount + commission_fee
        # 如果扣除手续费后金额不够的话就减掉100股
        if self.curr_amount < need_deduct_amount:
            if buy_quantity <= 100:
                return
            can_buy_total_quantity = floor((self.curr_amount - commission_fee) / price)
            buy_quantity = int(can_buy_total_quantity * position // 100) * 100
            if buy_quantity < 100:
                return
            buy_amount = buy_quantity * price
            commission_fee = self.commission.calc(side=SideEnum.SideEnum.BUY, amount=buy_amount)
            need_deduct_amount = buy_amount + commission_fee
            if self.curr_amount < need_deduct_amount:
                return
        # 余额扣除花费的金额
        self.curr_amount -= (buy_amount + commission_fee)
        # 增加持仓
        curr_quantity = self.stock_position_mapping.get(stock)
        if curr_quantity is None:
            curr_quantity = 0
        curr_quantity += buy_quantity
        self.stock_position_mapping[stock] = curr_quantity
        # 记录日志
        self.log(stock, time, price, buy_quantity, SideEnum.SideEnum.BUY, self.curr_amount)

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
        self.curr_amount = self.curr_amount + sell_amount - self.commission.calc(side=SideEnum.SideEnum.SELL, amount=sell_amount)
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
        self.log(stock, time, price, sell_quantity, SideEnum.SideEnum.SELL, self.curr_amount)

    # 判断是否有某只股票的持仓，默认查所有持仓
    def exist_position(self, stock: str = None):
        if stock is not None:
            return self.stock_position_mapping.get(stock) is not None
        return len(self.stock_position_mapping) > 0

    # 清空持仓
    def clean_positions(self, time: datetime):
        for stock, data in self.stock_position_mapping.items():
            self.sell(stock, time)

    def log(self, stock, time: datetime, price, quantity, side: SideEnum, currAmount: float):
        if self.open_log is False:
            return
        print(f'[{time}]{side}:{stock},价格:{price},数量:{quantity},余额:{currAmount}...')
