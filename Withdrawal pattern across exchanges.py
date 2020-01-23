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


def read_table(cur,sql_order):
    cur.execute(sql_order)
    data = cur.fetchall()
    frame = pd.DataFrame(list(data),columns = ['Hash_id','Block_id','UTC','From_address','To_address','Eth_Value'])
    return frame
def value(x):
    x = x.replace(',','') #将千位标识符逗号去掉，把ether也去掉
    return float(x[:-5])

def set_Exchange(address): #添加新列锁定地址的交易所
    for i in range(8):
        if address in subexchange_transfer_address[i]:
            return Exchange[i]
    return None
def set_day_and_night(UTC): # 添加新列锁定交易发生的时间段，其中6:00-12:00为上午，18：00-24:00为晚上,0:00-6:00为凌晨，
                            # 其余为白天
    if ((int(UTC.strftime('%H'))  >= 6) & (int(UTC.strftime('%H')) <= 12)):
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

#初始化 II
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

#withdrawal模式DF
withdrawal_transfer = pd.DataFrame({'Hash_id':[],'Block_id':[],'UTC':[],'From_address':[], \
                                 'To_address':[],'Eth_Value':[],'withdrawal_origin':[]})
for j in range(8):
    withdrawal_pattern = Eth_table_two_weeks[Eth_table_two_weeks['From_address'].isin(subexchange_transfer_address[j])] #此时寻找的是到地址为中介地址的交易
    withdrawal_pattern['withdrawal_origin'] = Exchange[j]
    withdrawal_pattern = withdrawal_pattern.reset_index(drop = False)
    withdrawal_transfer = withdrawal_transfer.append(withdrawal_pattern,ignore_index=True)

#散点图
# fig_withdrawal = px.scatter(withdrawal_transfer, x="UTC", y="Eth_Value", color="withdrawal_origin",marginal_x="histogram")
# fig_withdrawal.update(layout = dict(xaxis = dict(title = dict(text = 'Date', font =dict(size= 17)),tickfont = dict(size = 15)), \
#                            yaxis = dict(title = dict(text = 'Eth Value(Ether)', font =dict(size= 17)), tickfont = dict(size = 15)),\
#             title = dict(text = 'Withdrawal pattern across exchanges', font =dict(size= 20), xanchor = "auto")))
# fig_withdrawal.show()

#热图DF配置
withdrawal_transfer['Date'] = withdrawal_transfer.apply(lambda x: set_Date(x['UTC']),axis = 1)
withdrawal_transfer_22 = withdrawal_transfer[withdrawal_transfer["Date"] == '2019-11-22']
heatmap_22_withdrawal = pd.DataFrame({'withdrawal_origin':[],'Eth_Value':[],'hour':[]})
for hour in range(12):
    withdrawal_hour = withdrawal_transfer_22[(withdrawal_transfer_22['UTC'] >= datetime.datetime.strptime(hour_list_22[hour], "%Y-%m-%d %H:%M:%S")) & \
                              (withdrawal_transfer_22['UTC'] <= datetime.datetime.strptime(hour_list_22[hour + 1], "%Y-%m-%d %H:%M:%S"))].groupby('withdrawal_origin').sum()
    withdrawal_hour['hour'] = hour_list_22[hour]
    withdrawal_hour = withdrawal_hour.reset_index(drop = False)
    heatmap_22_withdrawal = heatmap_22_withdrawal.append(withdrawal_hour,ignore_index=True)
heatmap_22_withdrawal = heatmap_22_withdrawal.pivot('withdrawal_origin','hour','Eth_Value')

#画热图
sns.set(style='whitegrid',font_scale=1.4)
plt.figure(figsize = (15,8))
ax = sns.heatmap(heatmap_22_withdrawal,linewidths = 0.12, vmax=10000, vmin=0, center=1000,cmap = 'rainbow',
                linecolor = 'grey',robust = True)
ax.set_xticklabels(hour_list_22,rotation=45)
ax.set_yticklabels(['Binance','Bitfinex','Gemini','Huobi','Kraken','Poloniex'],rotation=0)
ax.set_xlabel('Hour')
ax.set_ylabel('Withdrawal origin')
ax.set_title('Withdrawal pattern transfer value analysis')
plt.show()
