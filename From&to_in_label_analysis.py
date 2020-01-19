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


#初始化设置
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

# From与To地址均属于label address的交易筛选
Transfer_between_Exchanges = Eth_table_two_weeks[((Eth_table_two_weeks["From_address"].isin(subexchange_total)) & (Eth_table_two_weeks["To_address"].isin(subexchange_total)))]
Transfer_between_Exchanges['From_address'] =Transfer_between_Exchanges.apply(lambda x: set_Exchange(x['From_address']),axis = 1)
Transfer_between_Exchanges['To_address'] =Transfer_between_Exchanges.apply(lambda x: set_Exchange(x['To_address']),axis = 1)
Transfer_between_Exchanges['Time'] = Transfer_between_Exchanges.apply(lambda x: set_day_and_night(x['UTC']),axis = 1)

a = px.scatter(Transfer_between_Exchanges, x="UTC", y="Eth_Value", color="From_address", marginal_x="histogram")
a.update_layout(xaxis={"title":"Date"},yaxis={"title":"Eth Value(Ether)"},)
a.update(layout=dict(annotations=[go.layout.Annotation(text="Huge one",x = '2019-11-13 18:19:08', y =87855)]))
#上面一行是给某一个点做注释的，需要输入x，y的坐标，而x，y的坐标设置需要考虑原本的坐标轴单位格式，时间等
# 时间转换时间戳
# timeArray = time.strptime('2019-11-13 18:19:08',"%Y-%m-%d %H:%M:%S")
# timeStamp = int(time.mktime(timeArray))
# print(timeStamp)

a.show()