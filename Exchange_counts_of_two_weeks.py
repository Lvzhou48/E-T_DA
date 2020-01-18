import pandas as pd
import pymysql
import seaborn as sns
import datetime
import matplotlib.pyplot as plt

def read_table(cur,sql_order):
    cur.execute(sql_order)
    data = cur.fetchall()
    frame = pd.DataFrame(list(data),columns = ['Hash_id','Block_id','UTC','From_address','To_address','Eth_Value'])
    return frame

conn = pymysql.connect(host = '127.0.0.1', port = 3306, user = 'root', passwd='huacheng11015',db = 'etherscan')#db是数据库
cursor = conn.cursor()
Eth_address = pd.read_excel('Get exchanges addresses labels.xlsx')
Exchange_trans_dict = {}
subexchange_transfer_address = []
Eth_table_two_weeks = read_table(cursor,"select * from eth_analyse Order By UTC ASC;")

Exchange = ['Binance','Bitfinex','Bithumb','Gemini','Huobi','Kraken','Okex','Poloniex']
for i in range(8):
    subexchange_transfer_address.append([])
    for _ in (Eth_address[Eth_address['Exchange'] == Exchange[i]]['Address']):
        subexchange_transfer_address[i].append(_)

for i in range(8):
    Exchange_trans_dict[Exchange[i]] = len(Eth_table_two_weeks[Eth_table_two_weeks['From_address'].isin(subexchange_transfer_address[i])]) + len(Eth_table_two_weeks[Eth_table_two_weeks['To_address'].isin(subexchange_transfer_address[i])])
    #print(Exchange_trans_dict)
Exchange_trans_Series = pd.Series(Exchange_trans_dict)

sns.set(style="darkgrid",font_scale=1.6)
sns.barplot(x=Exchange_trans_Series.index, y=Exchange_trans_Series.values)
plt.show()