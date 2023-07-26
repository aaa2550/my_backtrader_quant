import datetime

import pandas as pd
from pandas import DataFrame

from CommissionFeeChina import CommissionFeeChina
from CommissionInterface import CommissionInterface
from DataHandler import DataHandler
from QuantBotBase import QuantBotBase


class ContinueRisingBot(QuantBotBase):
    data_handler: DataHandler = None

    def __init__(self, config_path: str = '~/stock', initial_amount: float = 1000000,
                 start_time: datetime = datetime.datetime(2015, 1, 1),
                 end_time: datetime = datetime.datetime(2023, 6, 19),
                 stocks: list[str] = None, open_log: bool = True, use_cache=True,
                 commission: CommissionInterface = CommissionFeeChina(), bot_use_cache: bool = True,
                 max_positions: int = 1):
        self.data_handler = DataHandler(use_cache=use_cache)
        super().__init__(stock_up_date_map=self.data_handler.stock_up_date_map, stock_line_mapping=self.data_handler.stock_line_mapping, config_path=config_path,
                         initial_amount=initial_amount, start_time=start_time, end_time=end_time, stocks=stocks,
                         open_log=open_log, commission=commission, use_cache=bot_use_cache, max_positions=max_positions)

    def next(self, time: datetime, row, stock: str, stock_datas: DataFrame = None, total_position_amount: float = None):
        top_stocks = self.data_handler.heap_top_mapping.get(time)
        first_stock = next((stock.left for stock in top_stocks[:self.disuse.maxlen] if stock.left not in self.disuse), None)
        hit = first_stock == stock
        # 检查是否持仓
        if not self.exist_position(stock):  # 没有持仓
            if self.get_position_len() >= self.max_positions:
                return

            # 不是需要操作的股票则不做任何操作
            # if not hit:
            #     return
            # 否则买入
            if stock_datas[stock_datas.index >= time]['low'].min() <= 0:
                return

            self.buy(stock, time, total_position_amount)
        elif self.exist_position(stock):  # 否则如果当前的股票在持仓列表当中
            # 获取K线
            position = self.stock_position_mapping.get(stock)
            buyin_price = position['price']

            # 取15日最高价
            # days_max_price = stock_datas.loc[time - pd.DateOffset(days=15):time, 'high'].max()
            days_min_price_3 = stock_datas.loc[time - pd.DateOffset(days=3):time, 'low'].max()

            # 5、10、20均线
            ma5 = row._11
            ma10 = row._12
            ma20 = row._13

            # 趋势为多头继续持有
            if row.close > ma5:
                return

            # 现价 / 成本
            decline_percent = (row.close / buyin_price) - 1
            if decline_percent <= -0.05: #跌幅超过5%卖出
                self.sell(stock, time, total_position_amount, 1)
            elif decline_percent >= 0.1: # 盈利超过15% 卖出
                self.sell(stock, time, total_position_amount, 1)
            # elif row.close < days_min_price_3 : # 跌破3日线最低位
            #     self.sell(stock, time, total_position_amount, 1)
            elif row.close < ma20 : #跌破10日线
                self.sell(stock, time, total_position_amount, 1)

