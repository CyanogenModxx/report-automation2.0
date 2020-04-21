import pandas as pd
import pymysql
import datetime
from sqlalchemy import create_engine
import time

#获取昨天的日期,转换成时间戳
today = datetime.date.today()
if  today.day !=1:
    yestoday_start_time = (today-datetime.timedelta(1)).__format__('%Y-%m-%d') + ' 00:00:00'
    timeArray1 = time.strptime(yestoday_start_time, "%Y-%m-%d %H:%M:%S")
    yestoday_start  = time.mktime(timeArray1)
    yestoday_end_time = today.__format__('%Y-%m-%d') + ' 00:00:00'
    timeArray2 = time.strptime(yestoday_end_time, "%Y-%m-%d %H:%M:%S")
    yestoday_end  = int(time.mktime(timeArray2))
else :
    yestoday_start_time = (datetime.date(today.year,today.month,1)-datetime.timedelta(1)).__format__('%Y-%m-%d')+' 00:00:00'
    timeArray1 = time.strptime(yestoday_start_time, "%Y-%m-%d %H:%M:%S")
    yestoday_start = time.mktime(timeArray1)
    yestoday_end_time = today.__format__('%Y-%m-%d')+' 00:00:00'
    timeArray2 = time.strptime(yestoday_end_time, "%Y-%m-%d %H:%M:%S")
    yestoday_end = int(time.mktime(timeArray2))


def Data_transfer(database_info):
    #database_info是一个包含数据库服务器连接信息的DataFrame，构建一个迭代器将服务器信息以index,Series的形式遍历
    for i,row in database_info.iterrows():
        #连接数据库
        DB = pymysql.connect(row['IP'],row['Servername'],row['Password'],row['database'])
        cursor = DB.cursor()
        #查询用户信息
        customer_sql = "select email,uid,reg_time from zcshop_members where reg_time between %d and %d and email not like '%%taoten%%'AND email not like '%%@qq%%' AND email not like '%%adimin%%' AND email not like '%%@163.com%%' AND email not like '%%@126.com%%'"%(yestoday_start,yestoday_end)
        cursor.execute(customer_sql)
        #获取用户信息查询结果（结果是一个二维元组，将其转化成二维列表，再转化成DataFrame）
        customer_data = pd.DataFrame(list(cursor.fetchall()))
        if customer_data.empty== False:
            customer_data.columns=['email','uid','reg_time']
        else:
            print('%s客户信息的行数是:%d'%(i,len(customer_data)))
        #查询订单信息
        order_sql = "select orderid,o.uid,email,o.add_time,flid,payment,pay_time,money,device from zcshop_orders as o left join zcshop_members as m on o.uid=m.uid  where o.add_time between %d and %d and email not like '%%taoten%%'AND email not like '%%@qq%%' AND email not like '%%adimin%%' AND email not like '%%@163.com%%' AND email not like '%%@126.com%%' "%(yestoday_start,yestoday_end)

        cursor.execute(order_sql)
        #获取订单信息查询结果（结果是一个二维元组，将其转化成二维列表，再转化成DataFrame）
        order_data = pd.DataFrame(list(cursor.fetchall()))
        if order_data.empty==False:
            order_data.columns = ['orderid','uid','email','add_time','flid','payment','pay_time','money','device'] 
        else:
            print('%s订单信息的行数是:%d'%(i,len(order_data)))
        DB.close()
        
        
        #  将查询结果（DataFrame）批量写入本地Mysql
        engine = create_engine('mysql+pymysql://report_auto:abadnhDt2RjOGOQ6@localhost:3306/report_automation')
        if customer_data.empty==False:
            customer_data.to_sql(i+'_'+'customer',con = engine,if_exists='append',index=False)
            print('%s客户信息成功写入本地Mysql'%i)
        else:
            pass
            print('%s客户信息是空，取消写入数据库')
        if order_data.empty==False:
            order_data.to_sql(i+'_'+'order',con = engine,if_exists='append',index=False)
            print('%s订单信息成功写入本地Mysql'%i)
        else:
            pass
            print('%s订单信息是空，取消写入数据库'%i)
    return None
    
if __name__ == "__main__":
    ESlist = ['37.0.127.5','server1_dadan','PkEaxaOkpL9gSxJa','edmondsoccershop']
    GGlist = ['37.0.127.10','server4_dadan','PkEaxaOkpL9gSxJa88','gogoalshopvip']
    SDlist = ['37.0.127.10','server4_dadan','PkEaxaOkpL9gSxJa88','soccerdealshop']
    MJlist = ['185.16.212.6','server2_dadan','vfcV21yA1A65gupV','minejerseysvip']
    GJlist = ['185.16.212.6','server2_dadan','vfcV21yA1A65gupV','goaljerseys']
    
    database_info = pd.DataFrame(data=[ESlist,GGlist,SDlist,MJlist,GJlist],
                             columns = ['IP','Servername','Password','database'],
                            index = ['es','gg','sd','mj','gj'])
    Data_transfer(database_info)