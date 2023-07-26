import math
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime, timedelta
from math import floor
from typing import Dict

import akshare as ak
import pandas as pd
from pandas import DataFrame

import CommissionInterface
from CommissionFeeChina import CommissionFeeChina
from Common import Side
from Config import pers_path
from ResultView import ResultView
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
    # 最大持仓数量
    max_positions: int = 1
    # 费率
    commission: CommissionInterface = None
    one_cand_stocks_data_cache: Dict[datetime, DataFrame] = {}
    out_result: bool = True
    use_cache: bool = False
    disuse: deque = deque(maxlen=3)
    start_time: datetime = None
    end_time: datetime = None
    result_view: ResultView = None
    stock_up_date_map: dict[str: datetime] = None

    def __init__(self, stock_up_date_map: dict[str: datetime], stock_line_mapping: Dict[str, DataFrame], config_path: str = '~/stock',
                 initial_amount: float = 10000,
                 start_time: datetime = None, end_time: datetime = None, stocks: list[str] = None,
                 open_log: bool = True, out_result: bool = True, commission: CommissionInterface = CommissionFeeChina(),
                 use_cache: bool = True, max_positions: int = 1):
        self.config_path = config_path
        self.stock_line_mapping = stock_line_mapping
        self.initial_amount = initial_amount
        self.curr_amount = initial_amount
        self.out_result = out_result
        self.commission = commission
        self.use_cache = use_cache
        self.max_positions = max_positions
        self.start_time = start_time
        self.end_time = end_time
        self.stock_up_date_map = stock_up_date_map
        calendar = ak.tool_trade_date_hist_sina()
        calendar['trade_date'] = pd.to_datetime(calendar['trade_date'])
        calendar = calendar[
            (calendar['trade_date'] >= start_time) & (True if end_time is None else calendar['trade_date'] <= end_time)]
        calendar['trade_date'] = pd.to_datetime(calendar['trade_date'])
        calendar = calendar[calendar['trade_date'] <= pd.to_datetime('2023-06-20')]
        self.calendar = calendar['trade_date'].tolist()
        self.stocks = self.load_stocks(stocks)
        self.open_log = open_log
        self.load_one_cand_stocks_data()

        if self.out_result:
            self.result_view = ResultView()

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
            one_cand_data = self.one_cand_stocks_data_cache.get(cand)
            pos_map = self.stock_position_mapping
            total_position_amount = None
            # 如何开启结果展示，则追加结果信息
            if self.out_result and len(self.stock_position_mapping) > 0:
                total_position_amount = self.append_result_other_datas(cand, pos_map, one_cand_data)

            # 走下一个k线所有的股票
            self.next_cand_day(cand, one_cand_data, total_position_amount)

        print(f'清仓开始...')
        # 清仓看最后的结果
        self.clean_positions(self.calendar[-1])

        if self.out_result:
            self.build_result_view()

    def build_result_view(self):
        df = ak.index_zh_a_hist(symbol="000001", period='daily',
                                start_date=self.start_time.strftime("%Y%m%d"),
                                end_date=self.end_time.strftime("%Y%m%d"))
        df.rename(columns={'开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'vlos',
                           '日期': 'date'}, inplace=True)
        self.result_view.render(df, self.initial_amount, self.curr_amount)

    def append_result_other_datas(self, cand: datetime, pos_map: Dict[str, int], one_cand_data: DataFrame):
        total_position_amount = None
        try:
            position_amount = map(
                lambda stock:
                one_cand_data.loc[one_cand_data.index == stock, "close"].values[0] * pos_map[stock]['amount'],
                pos_map)
            total_position_amount = sum(position_amount)
        except Exception as e:
            pass
        if total_position_amount is not None:
            total_position_amount += self.curr_amount
        self.result_view.append_other(cand, set(pos_map.keys()), total_position_amount)
        return total_position_amount

    # 走下一个k线所有的股票
    def next_cand_day(self, cand: datetime, one_cand_data: DataFrame, total_position_amount: float):
        if one_cand_data is None:
            print(f"居然没有:{cand}")
            return
        for row in one_cand_data.itertuples():
            index = row.Index
            up_date = self.stock_up_date_map.get(index)
            if up_date is not None and (cand - up_date) <= timedelta(days=60):
                continue
            self.next(cand, row, index, self.stock_line_mapping.get(index), total_position_amount)

    # cand  k线日历
    # one_cand_stock 行数据
    # stock 股票代码
    # stock_datas 当前股票所有数据
    @abstractmethod
    def next(self, cand: datetime, row, stock: str, stock_datas: DataFrame = None, total_position_amount: float = None):
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
    def buy(self, stock: str, time: datetime, total_position_amount: float, position_rate: float = 1.0):
        # 全仓时为1，不能大于1
        if position_rate > 1:
            position_rate = 1
        # 获取股票k线
        stock_data = self.stock_line_mapping.get(stock)
        # 取出当前k线
        # stock_cand_data = stock_data.iloc[stock_data.index.get_loc(time) + 1]
        # if stock_cand_data is None:
        #     raise ValueError(f"{stock} at {time} time failed.")
        #
        # price = stock_cand_data['open']
        stock_cand_data = stock_data.loc[time]
        if stock_cand_data is None:
            raise ValueError(f"{stock} at {time} time failed.")

        price = stock_cand_data['close']
        if stock_cand_data['up_percent'] > 1 and stock_cand_data['open'] == stock_cand_data['close']:
            return
        if math.isnan(price):
            return
        # if price <= 2:
        #     return

        ma5 = stock_cand_data.loc['5ma']
        ma10 = stock_cand_data.loc['10ma']
        ma20 = stock_cand_data.loc['20ma']
        # 52 周最高价
        days_max_price = stock_data.loc[time - pd.DateOffset(weeks=52):time, 'high'].max()
        close = stock_cand_data['close']
        margin = stock_cand_data.loc['margin']
        diff = (close / days_max_price) - 1
        if (ma5 > ma10) and margin > 2 and close > ma5 and (close >= days_max_price or diff < -0.15):
            # 计算当前余额一共能买的总量
            can_buy_total_quantity = floor(self.curr_amount / price)
            # A股最少买100股，并且股数要是100的整数
            buy_quantity = int(can_buy_total_quantity * position_rate // 100) * 100
            if buy_quantity < 100:
                return
            # 买入的金额
            buy_amount = buy_quantity * price
            commission_fee = self.commission.calc(side=Side.BUY, amount=buy_amount)
            # 加手续费后需要扣除的总金额
            need_deduct_amount = buy_amount + commission_fee
            # 如果扣除手续费后金额不够的话就减掉100股
            if self.curr_amount < need_deduct_amount:
                if buy_quantity <= 100:
                    return
                can_buy_total_quantity = floor((self.curr_amount - commission_fee) / price)
                buy_quantity = int(can_buy_total_quantity * position_rate // 100) * 100
                if buy_quantity < 100:
                    return
                buy_amount = buy_quantity * price
                commission_fee = self.commission.calc(side=Side.BUY, amount=buy_amount)
                need_deduct_amount = buy_amount + commission_fee
                if self.curr_amount < need_deduct_amount:
                    return
            # 余额扣除花费的金额
            self.curr_amount -= (buy_amount + commission_fee)
            # 增加持仓
            position = self.stock_position_mapping.get(stock)
            amount = position['amount'] if position is not None else 0
            amount += buy_quantity
            self.stock_position_mapping[stock] = {'amount': amount, 'price': price}
            # 记录日志
            self.log(stock, time, price, buy_quantity, Side.BUY, self.curr_amount, amount, total_position_amount)

    # 执行卖出
    def sell(self, stock: str, time: datetime, total_position_amount: float, position_rate: float = 1.0):
        # 全仓时为1，不能大于1
        if position_rate > 1:
            position_rate = 1
        # 如果当前没有持仓则不做任何操作
        position = self.stock_position_mapping.get(stock)
        if position is None:
            return

        amount = position['amount']
        # 获取K线
        stock_data = self.stock_line_mapping.get(stock)
        # 获取当天k线
        # stock_cand_data = stock_data.iloc[stock_data.index.get_loc(time) + 1]
        # if stock_cand_data is None:
        #     raise ValueError(f"{stock} at {time} time failed.")
        # price = stock_cand_data['open']
        stock_cand_data = stock_data.loc[time]
        if stock_cand_data is None:
            raise ValueError(f"{stock} at {time} time failed.")
        price = stock_cand_data['close']
        if math.isnan(price):
            return
        if stock_cand_data['open'] == stock_cand_data['close']:
            return
        # 卖出数量
        sell_quantity = int(amount * position_rate / 100) * 100
        # 如果计算后能卖出的量是0就全仓卖出
        if sell_quantity == 0:
            sell_quantity = amount
        # 卖出应得的金额金额
        sell_amount = sell_quantity * price
        # 当前余额等于卖出应得金额扣除手续费后的钱
        self.curr_amount = self.curr_amount + sell_amount - self.commission.calc(side=Side.SELL,
                                                                                 amount=sell_amount)
        # 减掉持仓，如果卖出后没有任何持仓则del
        amount -= sell_quantity
        if amount < 0:
            raise ValueError(f"{stock} at {time} sell_quantity > curr_quantity.sell_quantity={sell_quantity}, "
                             f"curr_quantity={amount}.")
        elif amount == 0:
            del self.stock_position_mapping[stock]
        else:
            self.stock_position_mapping[stock] = position
        if sell_quantity == 0:
            print(sell_quantity)

        self.disuse.append(stock)
        # 记录日志
        self.log(stock, time, price, sell_quantity, Side.SELL, self.curr_amount, amount, total_position_amount)

    # 判断是否有某只股票的持仓，默认查所有持仓
    def exist_position(self, stock: str = None):
        if stock is not None:
            return self.stock_position_mapping.get(stock) is not None
        return len(self.stock_position_mapping) > 0

    def get_position_len(self):
        return len(self.stock_position_mapping)

    # 清空持仓
    def clean_positions(self, time: datetime, total_position_amount: float = None):
        for key in list(self.stock_position_mapping.keys()):
            self.sell(key, time, total_position_amount)

    def log(self, stock, time: datetime, price, quantity, side: Side, currAmount: float, amount: int
            , total_position_amount: float):
        content = f'[{time}]{side}:{stock},乘价:{price * quantity},价格:{price},数量:{quantity},账户总数量:{amount},余额:{round(currAmount, 2)}' \
                  f',总持仓金额:{round(total_position_amount, 2) if total_position_amount else None}'
        self.result_view.append_log(content)
        if self.open_log is False:
            return
        print(content)
