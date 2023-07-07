from abc import ABC

import SideEnum
from CommissionInterface import CommissionInterface


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
