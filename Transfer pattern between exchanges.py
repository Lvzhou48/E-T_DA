import pandas as pd
import pymysql
import seaborn as sns
import datetime
import matplotlib.pyplot as plt
import numpy as np
import plotly_express as px
import plotly
import plotly.graph_objs as go
import time


def read_table(cur, sql_order):
    cur.execute(sql_order)
    data = cur.fetchall()
    frame = pd.DataFrame(list(data), columns=['Hash_id', 'Block_id', 'UTC', 'From_address', 'To_address', 'Eth_Value'])
    return frame


def value(x):
    x = x.replace(',', '')  # 将千位标识符逗号去掉，把ether也去掉
    return float(x[:-5])


def set_Exchange(address):  # 添加新列锁定地址的交易所
    for i in range(8):
        if address in subexchange_transfer_address[i]:
            return Exchange[i]
    return None


def set_day_and_night(UTC):  # 添加新列锁定交易发生的时间段，其中6:00-12:00为上午，18：00-24:00为晚上,0:00-6:00为凌晨，
    # 其余为白天
    if ((int(UTC.strftime('%H')) >= 6) & (int(UTC.strftime('%H')) <= 12)):
        return 'forenoon'
    elif (int(UTC.strftime('%H')) >= 18):
        return 'night'
    elif (int(UTC.strftime('%H')) < 6):
        return 'wee'
    else:
        return 'afternoon'


def set_Date(UTC):
    for i in range(14):
        if (int(UTC.strftime('%d')) >= day_list[i]) & (int(UTC.strftime('%d')) < day_list[i + 1]):
            return time_list[i]


def set_hour(UTC):
    return str(datetime.datetime.strptime(UTC.strftime('%H') + '-00', '%H-%M').hour)

# 初始化 I
conn = pymysql.connect(host = '127.0.0.1', port = 3306, user = 'root', passwd='huacheng11015',db = 'etherscan')#db是数据库
cursor = conn.cursor()
Eth_address = pd.read_excel('Get exchanges addresses labels.xlsx')
Eth_address['Balance'] = Eth_address['Balance'].apply(lambda x:value(x))
subexchange_total = Eth_address['Address'].tolist()
Eth_table_two_weeks = read_table(cursor,"select * from eth_analyse Order By UTC ASC;")
#建立一个列表，每个交易所分配一个子列表，存放其他地址
subexchange_transfer_address = []
Exchange = ['Binance','Bitfinex','Bithumb','Gemini','Huobi','Kraken','Okex','Poloniex']
for i in range(8):
    subexchange_transfer_address.append([])
    for _ in (Eth_address[Eth_address['Exchange'] == Exchange[i]]['Address']):
        subexchange_transfer_address[i].append(_)
# 初始化 II
time_interval_list = ['2019-11-13 00:00:00','2019-11-14 00:00:00','2019-11-15 00:00:00','2019-11-16 00:00:00',
                     '2019-11-17 00:00:00','2019-11-18 00:00:00','2019-11-19 00:00:00','2019-11-20 00:00:00',
                     '2019-11-21 00:00:00','2019-11-22 00:00:00','2019-11-23 00:00:00','2019-11-24 00:00:00',
                     '2019-11-25 00:00:00','2019-11-26 00:00:00','2019-11-27 00:00:00']
day_list = [13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]
hour_list_22 = ['2019-11-22 00:00:00','2019-11-22 02:00:00',
               '2019-11-22 04:00:00','2019-11-22 06:00:00',
               '2019-11-22 08:00:00','2019-11-22 10:00:00',
               '2019-11-22 12:00:00','2019-11-22 14:00:00',
               '2019-11-22 16:00:00','2019-11-22 18:00:00',
               '2019-11-22 20:00:00','2019-11-22 22:00:00',
               '2019-11-23 00:00:00']
time_list = ['2019-11-13','2019-11-14','2019-11-15','2019-11-16',
                     '2019-11-17','2019-11-18','2019-11-19','2019-11-20',
                     '2019-11-21','2019-11-22','2019-11-23','2019-11-24',
                     '2019-11-25','2019-11-26','2019-11-27']
#删除From To都在label地址里的交易记录
Eth_table_two_weeks = Eth_table_two_weeks[~((Eth_table_two_weeks["From_address"].isin(subexchange_total)) & (Eth_table_two_weeks["To_address"].isin(subexchange_total)))]
Eth_table_two_weeks = Eth_table_two_weeks.reset_index(drop = False)

#建立 deposit模式的中间地址列表 intermediary_address
intermediary_address = []
for i in range(8):
    #intermediary_address.append([])
    intermediary_address_exchange =  Eth_table_two_weeks[Eth_table_two_weeks['To_address'].isin(subexchange_transfer_address[i])]['From_address'].tolist()
    intermediary_address_exchange = list(set(intermediary_address_exchange))
    intermediary_address.append(intermediary_address_exchange)
len(intermediary_address)
#跨交易所模式，寻找intermediary_address的左边是交易所地址的交易，标上To_destination目的地，再设置自己所在交易所
between_exchange_transfer = pd.DataFrame({'Hash_id':[],'Block_id':[],'UTC':[],'From_address':[], \
                                 'To_address':[],'Eth_Value':[],'To_destination':[]})
for k in range(8):
    Exchange_address_df = Eth_table_two_weeks[Eth_table_two_weeks['To_address'].isin(intermediary_address[k])]
    Exchange_address_df = Exchange_address_df[(Exchange_address_df['From_address'].isin(subexchange_total))]
    Exchange_address_df['To_destination'] = Exchange[k]
    Exchange_address_df = Exchange_address_df.reset_index(drop = False)
    between_exchange_transfer = between_exchange_transfer.append(Exchange_address_df,ignore_index=True)
between_exchange_transfer['From_origin'] = between_exchange_transfer.apply(lambda x: set_Exchange(x['From_address']),axis = 1)

#pivot 图
between_exchange_transfer['count'] = [1 for i in range(len(between_exchange_transfer))]
between_exchange_transfer_pivot = pd.DataFrame.pivot_table(between_exchange_transfer,values='Eth_Value',index = ['From_origin'],columns = ['To_destination'],  \
                         aggfunc = np.sum)
between_exchange_transfer_pivot.to_excel('between_exchange_transfer_pivot.xlsx')

between_exchange_transfer['count'] = [1 for i in range(len(between_exchange_transfer))]
between_exchange_transfer_pivot_count = pd.DataFrame.pivot_table(between_exchange_transfer,values='count',index = ['From_origin'],columns = ['To_destination'],  \
                         aggfunc = np.sum)
between_exchange_transfer_pivot_count.to_excel('between_exchange_transfer_pivot_count.xlsx')

#根据count画桑基图
fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = ['Binance_ori','Bitfinex_ori','Gemini_ori','Huobi_ori','Kraken_ori','Poloniex_ori', \
              'Binance_des','Bitfinex_des','Gemini_des','Huobi_des','Kraken_des','Poloniex_des'],
      color = ["Red","Yellow","green","Blue","Purple","Brown","Red","Yellow","green","Blue","Purple","Brown"]
    ),
    link = dict(
      source = [0,0,0,0,0,0,1,1,1,1,1,1,2,2,2,2,2,2,3,3,3,3,3,3,4,4,4,4,4,4,5,5,5,5,5,5], #从上往下每列依次为，label[source[i]]到label[target[i]]的流数量有label[value[i]]
      target = [6,7,8,9,10,11,6,7,8,9,10,11,6,7,8,9,10,11,6,7,8,9,10,11,6,7,8,9,10,11,6,7,8,9,10,11,],
      value = [843,166,140,127,570,155,248,0,8,26,102,36,150,13,71,0,37,33,68,33,0,1,1,2,366,120,44,6,0,49,36,4,1,3,3,0]
  ))])

fig.update_layout(title_text="Transfer pattern between exchanges Sankey Diagram", font_size=15)
#fig.show(renderer = 'png', width = 1200, height = 600)
fig.show()