# encoding: utf-8
import pymysql
import numpy as np
import pandas as pd






from sqlalchemy import create_engine

def Data_transfer(database_info):
    #database_info是一个包含数据库服务器连接信息的DataFrame，构建一个迭代器将服务器信息以index,Series的形式遍历
    for i,row in database_info.iterrows():
        #连接数据库
        DB = pymysql.connect(row['IP'],row['Servername'],row['Password'],row['database'])
        cursor = DB.cursor()
        #查询用户信息
        customer_sql = "select email,uid,reg_time from zcshop_members where reg_time between 1546272000 and 1575129600 and email not like '%%taoten%%'AND email not like '%%@qq%%' AND email not like '%%adimin%%' AND email not like '%%@163.com%%' AND email not like '%%@126.com%%'"
        cursor.execute(customer_sql)
        #获取用户信息查询结果（结果是一个二维元组，将其转化成二维列表，再转化成DataFrame）
        customer_data = pd.DataFrame(list(cursor.fetchall()))
        customer_data.columns=['email','uid','reg_time']
        print('%s客户信息的行数是:%d'%(i,len(customer_data)))
        #查询订单信息
        order_sql = "select orderid,o.uid,email,o.add_time,flid,payment,pay_time,money,device from zcshop_orders as o left join zcshop_members as m on o.uid=m.uid  where o.add_time  between 1546272000 and 1575129600 and email not like '%%taoten%%'AND email not like '%%@qq%%' AND email not like '%%adimin%%' AND email not like '%%@163.com%%' AND email not like '%%@126.com%%' "
        
        cursor.execute(order_sql)
        #获取订单信息查询结果（结果是一个二维元组，将其转化成二维列表，再转化成DataFrame）
        order_data = pd.DataFrame(list(cursor.fetchall()))
        order_data.columns = ['orderid','uid','email','add_time','flid','payment','pay_time','money','device']
        print('%s订单信息的行数是:%d'%(i,len(order_data)))
        DB.close()
        
        #将查询结果（DataFrame）批量写入本地Mysql
        engine = create_engine('mysql+pymysql://report_auto:abadnhDt2RjOGOQ6@localhost:3306/report_automation')
        customer_data.to_sql(i+'_'+'customer',con = engine,if_exists='replace',index=False)
        print('%s客户信息成功写入本地Mysql'%i)
        order_data.to_sql(i+'_'+'order',con = engine,if_exists='replace',index=False)
        print('%s订单信息成功写入本地Mysql'%i)
    return ('全部网站数据写入成功')
    
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
    