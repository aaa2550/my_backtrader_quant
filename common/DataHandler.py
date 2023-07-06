from typing import Dict
from pandas import DataFrame
from common import Pair
import pandas as pd
import concurrent.futures as cf
import pickle
import os


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