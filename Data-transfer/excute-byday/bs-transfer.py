import pandas as pd
import pymysql
import datetime
from sqlalchemy import create_engine

#获取昨天的日期，格式："2019-01-01"
today = datetime.date.today()
if  today.day !=1:
    yestoday = (today-datetime.timedelta(1)).__format__('%Y-%m-%d')
else :
    yestoday = (datetime.date(today.year,today.month,1)-datetime.timedelta(1)).__format__('%Y-%m-%d')



def Data_transfer(database_info):
    #database_info是一个包含数据库服务器连接信息的DataFrame，构建一个迭代器将服务器信息以index,Series的形式遍历
    for i,row in database_info.iterrows():
        #连接数据库
        DB = pymysql.connect(row['IP'],row['Servername'],row['Password'],row['database'])
        cursor = DB.cursor()
        #查询用户信息
        customer_sql = "select email,id_customer,date_add from ps_customer where date_add like '%s%%' and email not like '%%taoten%%'AND email not like '%%@qq%%' AND email not like '%%adimin%%' AND email not like '%%@163.com%%' AND email not like '%%@126.com%%'  "%yestoday
        cursor.execute(customer_sql)
        #获取用户信息查询结果（结果是一个二维元组，将其转化成二维列表，再转化成DataFrame）
        customer_data = pd.DataFrame(list(cursor.fetchall()))
        customer_data.columns=['email','uid','reg_time']
        print('%s客户信息的行数是:%d'%(i,len(customer_data)))
        #查询订单信息
        order_sql = "select id_order,o.id_customer,email,o.date_add,current_state,payment,total_paid,total_paid_real,device from ps_orders as o left join ps_customer as m on o.id_customer=m.id_customer  where o.date_add like '%s%%' and email not like '%%taoten%%'AND email not like '%%@qq%%' AND email not like '%%adimin%%' AND email not like '%%@163.com%%' AND email not like '%%@126.com%%' "%yestoday
        
        cursor.execute(order_sql)
        #获取订单信息查询结果（结果是一个二维元组，将其转化成二维列表，再转化成DataFrame）
        order_data = pd.DataFrame(list(cursor.fetchall()))
        order_data.columns = ['orderid','uid','email','add_time','flid','payment','money','paid_money','device']
        print('%s订单信息的行数是:%d'%(i,len(order_data)))
        DB.close()
        
        #将查询结果（DataFrame）批量写入本地Mysql
        engine = create_engine('mysql+pymysql://report_auto:abadnhDt2RjOGOQ6@localhost:3306/report_automation')
        customer_data.to_sql(i+'_'+'customer',con = engine,if_exists='append',index=False)
        print('%s客户信息成功写入本地Mysql'%i)
        order_data.to_sql(i+'_'+'order',con = engine,if_exists='append',index=False)
        print('%s订单信息成功写入本地Mysql'%i)
    return ('全部网站数据写入成功')
    
if __name__ == "__main__":
    BSlist = ['37.0.127.9','luochao','vwzTkJgMQTJYctc6','bestsoccerstore']
    database_info = pd.DataFrame(data=[BSlist], 
                             columns = ['IP','Servername','Password','database'],
                            index = ['bs'])
    Data_transfer(database_info)
