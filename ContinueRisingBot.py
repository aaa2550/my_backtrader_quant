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
                 start_time: datetime = datetime.datetime(2010, 1, 1),
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
        first_stock = next((stock.key for stock in top_stocks[:self.disuse.maxlen] if stock.key not in self.disuse), None)
        hit = first_stock == stock
        # if top_stocks is not None:
        #     hit = stock in [pair.key for pair in top_stocks]
        # 检查是否持仓
        if not self.exist_position(stock):  # 没有持仓
            if self.get_position_len() >= self.max_positions:
                return

            # 不是需要操作的股票则不做任何操作
            if not hit:
                return
            # 否则买入
            if stock_datas[stock_datas.index >= time]['low'].min() <= 0:
                return

            self.buy(stock, time, total_position_amount)
        elif self.exist_position(stock):  # 否则如果当前的股票在持仓列表当中
            # 取30日最高价
            days_max_price = stock_datas.loc[time - pd.DateOffset(days=30):time, 'high'].max()
            # 收盘价站最高价下降百分比
            decline_percent = (days_max_price - row.close) / days_max_price
            # 如果大于15%卖出一半，如果大于20%全仓卖出
            if 0.15 < decline_percent < 0.2:
                self.sell(stock, time, total_position_amount, 0.5)
            elif decline_percent > 0.2:
                self.sell(stock, time, total_position_amount, 1)

