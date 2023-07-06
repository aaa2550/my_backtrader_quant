from pandas import DataFrame

import Utils


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