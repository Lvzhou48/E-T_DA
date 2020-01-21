import pandas as pd
import pymysql
import seaborn as sns
import datetime
import matplotlib as plt
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


# 通过 intermediary_address 寻找各交易所的deposit交易Dataframe，添加destination列标识最终归集到哪个交易所

deposit_transfer = pd.DataFrame({'Hash_id':[],'Block_id':[],'UTC':[],'From_address':[], \
                                 'To_address':[],'Eth_Value':[],'deposit_destination':[]})
for j in range(8):
    User_address_df = Eth_table_two_weeks[Eth_table_two_weeks['To_address'].isin(intermediary_address[j])]
    User_address_df = User_address_df[~(User_address_df['From_address'].isin(subexchange_total))]
    User_address_df['deposit_destination'] = Exchange[j]
    User_address_df = User_address_df.reset_index(drop = False)
    deposit_transfer = deposit_transfer.append(User_address_df,ignore_index=True)

#散点图
fig_deposit = px.scatter(deposit_transfer, x="UTC", y="Eth_Value", color="deposit_destination",title = 'Deposit pattern across exchanges')
#fig.update(dict(layout=dict(title = 'Deposit pattern across exchanges',x = 0.5) ))
fig_deposit.update(layout = dict(xaxis = dict(title = dict(text = 'Date', font =dict(size= 17)),tickfont = dict(size = 15)), \
                           yaxis = dict(title = dict(text = 'Eth Value(Ether)', font =dict(size= 17)), tickfont = dict(size = 15)),\
            title = dict(text = 'Deposit pattern across exchanges', font =dict(size= 20), xanchor = "auto")))
fig_deposit.show()

#
