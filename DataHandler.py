import datetime
from typing import Dict
import pandas as pd
from pandas import DataFrame

from Config import pers_path
from Triple import Triple
from Utils import do_percent, deserialize_data, serialize_data


class DataHandler:
    # 目录
    DIR = '~/stock'
    PERIOD = 120
    # 按日期每天存top的股票
    heap_top_mapping: Dict[str, list[Triple]] = {}
    stock_line_mapping: Dict[str, DataFrame] = {}
    # 缓存股票的csv避免重复load影响性能
    stock_csv_cache = None
    stock_up_date_map: dict[str: datetime] = None
    stocks = None

    def __init__(self, use_cache=True):
        self.stock_csv_cache = {}
        self.heap_top_mapping = {}
        self.load_stock_up_date_map()
        # 是否使用缓存
        if use_cache:
            print('使用缓存数据...')
            local_heap_top_mapping = deserialize_data(pers_path + '/local_heap_top_mapping.pkl')
            if local_heap_top_mapping is not None:
                self.heap_top_mapping = local_heap_top_mapping
            local_stock_line_mapping = deserialize_data(pers_path + '/stock_line_mapping.pkl')
            if local_stock_line_mapping is not None:
                self.stock_line_mapping = local_stock_line_mapping
            print('缓存加载数据完成...')
            return

        print('重新构建数据...')
        datas = self.load_stocks()

        count = len(datas)
        index = 0
        start = datetime.datetime.now()
        for stock in datas:
            print(f'load_stock_line_mapping:{stock}')
            do_percent(index, count, 'load_stock_line_mapping')
            self.load_stock_line_mapping(stock)
            index = index + 1

        serialize_data(self.stock_line_mapping, pers_path + '/stock_line_mapping.pkl')
        print(f'load_stock_line_mapping耗时：{datetime.datetime.now() - start}秒')

        print(f'load_stock_line_mapping执行结束...')

        start = datetime.datetime.now()
        index = 0
        for stock in datas:
            print(f'stock_line_mapping:{stock}')
            do_percent(index, count, 'load_top_by_day')
            data = self.stock_line_mapping.get(stock)
            self.load_top_by_day(stock, data)
            index = index + 1

        # 半年内涨幅排序后保留top10
        for key, value in self.heap_top_mapping.items():
            sorted_values = sorted(value, key=lambda p: p.middle, reverse=True)
            # 限制最后一天照最高点下降了的百分比
            # sorted_values = [item for item in sorted_values if item.right > 0.05]
            self.heap_top_mapping[key] = sorted_values[:10]

        serialize_data(self.heap_top_mapping, pers_path + '/local_heap_top_mapping.pkl')
        print(f'load_top_by_day耗时：{datetime.datetime.now() - start}秒')

        print('数据构建完成...')
        # 加载到缓存
        print('数据写入缓存完成...')

    @staticmethod
    def get_up_interval_or_extent(stock, day_data):
        day_data['5ma'] = day_data['close'].rolling(window=5, min_periods=5).mean()
        day_data['20ma'] = day_data['close'].rolling(window=20, min_periods=20).mean()
        day_data['ups'] = day_data['5ma'] > day_data['20ma']
        day_data['ups'] = (day_data['ups'] != day_data['ups'].shift()) | (day_data['ups'] == False)
        day_data['ups'] = day_data['ups'].cumsum()  # 辅助列，根据识别信号，对相邻的相同行为进行分组，便于计算每组相同行为的连续发生次数
        day_data['ups'] = day_data.groupby(['ups'])['date'].rank(method='dense').astype(
            int)  # 根据行为分组，使用窗口函数对每条行为标记连续发生次数

        up = [0, 0, 0]

        # 上涨了多少天和离最高点跌幅
        def up_percent_and_down_rate(row):
            window_size = int(row['ups'])
            low = row['low']
            high = row['high']
            close = row['close']
            down_rate = 100
            if window_size == 1:
                up[0] = low
                up[1] = high
            elif window_size > 1:
                if low < up[0]:
                    up[0] = low
                if high > up[1]:
                    up[1] = high
                if up[1] == 0:
                    down_rate = 100
                else:
                    down_rate = (up[1] - close) / up[1]
            else:
                raise Exception(f"window_size={window_size},stock={stock},row={row}")

            if up[0] == 0:
                return up[1], down_rate
            return (up[1] - up[0]) / abs(up[0]), down_rate

        day_data[['up_percent', 'down_rate']] = day_data.apply(lambda row: up_percent_and_down_rate(row), axis=1, result_type='expand')
        day_data.set_index('date', inplace=True)
        return day_data

    def load_stock_line_mapping(self, stock: str):
        data = self.load_stock(stock)
        data = self.get_up_interval_or_extent(stock, data)
        self.stock_line_mapping[stock] = data

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
        data['date'] = pd.to_datetime(data['date'])
        self.stock_csv_cache[stock] = data

        return data

    def load_top_by_day(self, stock, data):
        for row in data.itertuples():
            index = row.Index
            top_ = self.heap_top_mapping.get(index)
            if row.up_percent is None:
                print(f'stock:{stock},index:{index}')
            if top_ is None:
                top_ = []
                self.heap_top_mapping[index] = top_
            top_.append(Triple(stock, row.up_percent, row.down_rate))


    def load_stock_up_date_map(self):
        data = pd.read_csv(f'{self.DIR}/date.csv', header=0)
        data['date'] = pd.to_datetime(data['date'])
        self.stock_up_date_map = data.set_index('stock')['date'].to_dict()
