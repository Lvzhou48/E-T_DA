import pandas as pd
import pymysql
import seaborn as sns
import datetime
import matplotlib.pyplot as plt
import numpy as np


conn = pymysql.connect(host = '127.0.0.1', port = 3306, user = 'root', passwd='huacheng11015',db = 'etherscan')#db是数据库
cursor = conn.cursor()
Eth_address = pd.read_excel('Get exchanges addresses labels.xlsx')


#用address列来比对每个交易所的子交易地址，添加新列
def set_Exchange(address):
    for i in range(8):
        if address in subexchange_transfer_address[i]:
            return Exchange[i]
    return None

def read_table(cur,sql_order):
    cur.execute(sql_order)
    data = cur.fetchall()
    frame = pd.DataFrame(list(data),columns = ['Hash_id','Block_id','UTC','From_address','To_address','Eth_Value'])
    return frame

def value(x):
    x = x.replace(',','') #将千位标识符逗号去掉，把ether也去掉
    return float(x[:-5])
Eth_address['Balance'] = Eth_address['Balance'].apply(lambda x:value(x))


Eth_table_two_weeks = read_table(cursor,"select * from eth_analyse Order By UTC ASC;")

#建立一个列表，每个交易所分配一个子列表，存放其他地址
subexchange_transfer_address = []
Exchange = ['Binance','Bitfinex','Bithumb','Gemini','Huobi','Kraken','Okex','Poloniex']
for i in range(8):
    subexchange_transfer_address.append([])
    for _ in (Eth_address[Eth_address['Exchange'] == Exchange[i]]['Address']):
        subexchange_transfer_address[i].append(_)

#所有交易所子地址汇集subexchange_total,把所有与交易所有关的交易筛出为heatmap_From_data,heatmap_To_data
subexchange_total = []
for i in range(8):
    for j in range(len(subexchange_transfer_address[i])):
        subexchange_total.append(subexchange_transfer_address[i][j])
heatmap_From_data = Eth_table_two_weeks[Eth_table_two_weeks['From_address'].isin(subexchange_total)]
heatmap_To_data =  Eth_table_two_weeks[Eth_table_two_weeks['To_address'].isin(subexchange_total)]
heatmap_From_data = heatmap_From_data.reset_index(drop = True)
heatmap_To_data = heatmap_To_data.reset_index(drop = True)

# set_Exchange函数添加新列表示from或者to的地址所在的交易所
heatmap_From_data['From_Exchange'] = heatmap_From_data.apply(lambda x: set_Exchange(x['From_address']),axis = 1)
heatmap_To_data['To_Exchange'] = heatmap_To_data.apply(lambda x: set_Exchange(x['To_address']),axis = 1)

# 分成14个时间段以备后用
time_interval_list = ['2019-11-13 00:00:00','2019-11-14 00:00:00','2019-11-15 00:00:00','2019-11-16 00:00:00',
                     '2019-11-17 00:00:00','2019-11-18 00:00:00','2019-11-19 00:00:00','2019-11-20 00:00:00',
                     '2019-11-21 00:00:00','2019-11-22 00:00:00','2019-11-23 00:00:00','2019-11-24 00:00:00',
                     '2019-11-25 00:00:00','2019-11-26 00:00:00','2019-11-27 00:00:00']
time_list = ['2019-11-13','2019-11-14','2019-11-15','2019-11-16',
                     '2019-11-17','2019-11-18','2019-11-19','2019-11-20',
                     '2019-11-21','2019-11-22','2019-11-23','2019-11-24',
                     '2019-11-25','2019-11-26','2019-11-27']

# 将from和to的heatmap都设置好，根据时间间隔筛选，然后根据所在交易所聚合，
# 求Eth_Value的和，然后组建 交易所，金额和日期为三要素的新表
heatmap_From = pd.DataFrame({'From_Exchange':[],'Eth_Value':[],'date':[]})
heatmap_To = pd.DataFrame({'To_Exchange':[],'Eth_Value':[],'date':[]})
# for day in range(14):
#     From_day = heatmap_From_data[(heatmap_From_data['UTC'] >= time_interval_list[day]) & (heatmap_From_data['UTC'] <= \
#                 time_interval_list[day+1])].groupby('From_Exchange').sum()
#     From_day['date'] = time_list[day]
#     From_day = From_day.reset_index(drop = False)
#     heatmap_From = heatmap_From.append(From_day,ignore_index=True)
for day in range(14):
    To_day = heatmap_To_data[(heatmap_To_data['UTC'] >= time_interval_list[day]) & (heatmap_To_data['UTC'] <= \
                time_interval_list[day+1])].groupby('To_Exchange').sum()
    To_day['date'] = time_list[day]
    To_day = To_day.reset_index(drop = False)
    heatmap_To = heatmap_To.append(To_day,ignore_index=True)

#调整图像，画出heatmap
#heatmap_From = heatmap_From.pivot('From_Exchange', "date", "Eth_Value")
heatmap_To = heatmap_To.pivot('To_Exchange', "date", "Eth_Value")
sns.set(style='whitegrid',font_scale=1.2)
ax = sns.heatmap(heatmap_To,linewidths = 0.12, vmax=80000, vmin=0, center=35000,cmap = 'rainbow',
                linecolor = 'grey',robust = True)
Exchange_plot = ['Binance','Bitfinex','Gemini','Huobi','Kraken','Okex','Poloniex']
ax.set_xticklabels(time_list,rotation=45)
ax.set_yticklabels(Exchange_plot)
ax.set_xlabel('Date')
ax.set_ylabel('Exchange(To)')
plt.show()
