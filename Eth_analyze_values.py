import pandas as pd
import pymysql
import seaborn as sns
import datetime
import matplotlib.pyplot as plt

#将重新筛选完成后的数据存入etherscan数据库中，形成筛选后的analyze表

conn = pymysql.connect(host = '127.0.0.1', port = 3306, user = 'root', passwd='huacheng11015',db = 'etherscan')#db是数据库
cursor = conn.cursor()
Eth_address = pd.read_excel('Get exchanges addresses labels.xlsx')


def read_table(cur,sql_order):
    cur.execute(sql_order)
    data = cur.fetchall()
    frame = pd.DataFrame(list(data),columns = ['Hash_id','Block_id','UTC','From_address','To_address','Eth_Value'])
    return frame


Eth_table = read_table(cursor,"select * from eth_transfer Order By UTC ASC;")#先将数据排序,从sql中取出
Eth_table['UTC'] = pd.to_datetime(Eth_table['UTC'])


start_date = datetime.datetime.strptime('2019-11-13 00:00:00','%Y-%m-%d %H:%M:%S')
end_date = datetime.datetime.strptime('2019-11-27 00:00:00','%Y-%m-%d %H:%M:%S')
Eth_table_two_weeks = Eth_table[(Eth_table['UTC'] >= start_date) & (Eth_table['UTC'] <= end_date)]



Eth_table_two_weeks = Eth_table_two_weeks[ Eth_table_two_weeks['Eth_Value'] != 0 ]# 将所有Eth_Value 为0的交易记录都去掉

a = Eth_table_two_weeks[Eth_table_two_weeks['Eth_Value'] <= 10]
#Eth_table_two_weeks['Eth_Value'] <= 10
sns.distplot(a['Eth_Value'])
plt.show()





