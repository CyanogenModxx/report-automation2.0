import pandas as pd
import pymysql
import datetime
from sqlalchemy import create_engine
import time




DB = pymysql.connect('37.0.127.5','server1_dadan','PkEaxaOkpL9gSxJa','stock')
sql  = '''select id,sku,size,num,site,info,info_stock,order_id,statu,product_name,unit_price,total_amount,add_time from stock_size_log 
            where add_time like '2019-12-01%%' '''

df = pd.read_sql(sql,DB)

engine = create_engine('mysql+pymysql://report_auto:abadnhDt2RjOGOQ6@localhost:3306/report_automation')
df.to_sql('stock_log',con=engine,if_exists='append',index=False)

print ('stock log data write into mysql success')