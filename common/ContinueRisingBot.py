import datetime

import pandas as pd
from pandas import DataFrame

from common import DataHandler
from my_quant import QuantBotBase


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

