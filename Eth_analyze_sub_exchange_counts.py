import pandas as pd
import pymysql
import seaborn as sns
import datetime
import matplotlib.pyplot as plt

#
def value(x):
    x = x.replace(',','') #将千位标识符逗号去掉，把ether也去掉
    return float(x[:-5])


Eth_address = pd.read_excel('Get exchanges addresses labels.xlsx')
Eth_address['Balance'] = Eth_address['Balance'].apply(lambda x:value(x))
plt.style.use({'figure.figsize':(20, 15)})

sns.set(style="whitegrid",font_scale=1.8)
#ax = sns.barplot(x = Eth_address[Eth_address['Exchange'] == 'Huobi']['Sub_exchange'] , y = Eth_address['Balance'])
ax = sns.barplot(x = Eth_address[Eth_address['Exchange'] == 'Binance']['Sub_exchange'] , y = Eth_address['Balance'])
#ax.set_xticklabels(Eth_address['Sub_exchange'],rotation=45)
plt.show()