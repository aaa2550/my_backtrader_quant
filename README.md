# 介绍

## 需要的数据集
stock数据：需要有'代码'列
每个stock的数据：
    文件名:需要是纯stock的code，不能有前缀例如：sh000001(错误的数据), 000001(正确的数据)
    列:需要有date、open、close、high、low列
    复权:
        前复权：如果使用前复权可能存在历史价格为负数情况，回测时已经在程序中过滤掉负数情况
        后复权：如果使用后复权数据不会存在负数情况，但因除权导致的价格变化可能会对价格策略判断造成影响


