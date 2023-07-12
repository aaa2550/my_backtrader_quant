from datetime import datetime

import pandas as pd
from pandas import DataFrame

import Utils


class ResultView:
    template_url: str = None
    out_url: str = None
    k_line: list[list] = None
    categoryData: list[str] = None
    vols: list[float] = None
    amounts: list[float] = None
    other: list[list] = []
    max_draw_down: float = 0
    log: str = ''

    def __init__(self, template_url: str = './template.html', out_url: str = './result.html'):
        self.template_url = template_url
        self.out_url = out_url

    def parse(self, datas: DataFrame):
        datas = datas.reindex(columns=['open', 'high', 'low', 'close', 'vlos', 'date'])
        other_datas = pd.DataFrame(self.other, columns=['date', 'stocks', 'amount'])
        datas['date'] = pd.to_datetime(datas['date'])
        datas = pd.merge(datas, other_datas, on="date", how="left")
        datas['amount'].fillna(method='ffill', inplace=True)

        max_amount = None
        max_draw_down = 0
        for row in datas.itertuples():
            amount = row.amount
            if max_amount is None:
                max_amount = -1
            if amount > max_amount:
                max_amount = amount

            temp = (max_amount - amount) / max_amount
            print(f'amount:{amount}, max_amount:{max_amount}')
            if temp > max_draw_down:
                max_draw_down = temp

        self.max_draw_down = max_draw_down

        self.categoryData = datas['date'].tolist()
        self.vols = datas['vlos'].tolist()
        self.amounts = datas['amount'].tolist()

        datas = datas[['open', 'high', 'low', 'close']]
        self.k_line = datas.values.tolist()

    def append_other(self, time: datetime, stocks: set[str], total_quantity: int):
        self.other.append([time, stocks, total_quantity])

    def render(self, datas: DataFrame, init_amount: float, last_amount: float):
        self.parse(datas)
        content = Utils.read(self.template_url)
        category_data = self.build_category_data()
        k_line = self.build_k_line()
        vols = self.build_vols()
        amount = self.build_amount()
        print(f'self.max_draw_down:{self.max_draw_down}')
        content = content.replace("#{categoryData}", category_data) \
            .replace("#{values}", k_line) \
            .replace("#{vols}", vols) \
            .replace("#{amount}", amount) \
            .replace("#{maxDrawDown}", str(round(self.max_draw_down * 100, 2))) \
            .replace("#{initAmount}", str(init_amount)) \
            .replace("#{lastAmount}", str(round(last_amount, 2)))

        Utils.write(content, self.out_url)

    def build_category_data(self):
        content = ''
        for value in self.categoryData:
            content += f"'{value}',"
        return content.removesuffix(",")

    def build_k_line(self):
        content = ''
        for sub_list in self.k_line:
            content += f'[{sub_list[0]},{sub_list[1]},{sub_list[2]},{sub_list[3]}],'
        return content.removesuffix(",")

    def build_vols(self):
        content = ''
        for value in self.vols:
            content += f"'{value}',"
        return content.removesuffix(",")

    def build_amount(self):
        content = ''
        for value in self.amounts:
            content += f"'{value}',"
        return content.removesuffix(",")

    def append_log(self, content: str):
        self.log += content
