from flask import request
import time
import pymysql
from collections import OrderedDict
from sqlalchemy import create_engine
import numpy as np 
import pandas as pd 
from app import app
from config import execute_sql


#获取全部网站用户数、新用户数、会话数
start_time = 1546272000 #2019-01-01 00:00:00
now = time.localtime(time.time())
end_time = '%d-%d-01 00:00:00'%(now.tm_year,now.tm_mon)
end_time = int(time.mktime(time.strptime(end_time,'%Y-%m-%d %H:%M:%S')))
last_month = int('%d'%now.tm_year+'0'+'%d'%(now.tm_mon-1))

def get_allsite_uns(start_time=start_time,end_time=end_time):
    
    count = 0
    for s in ['bs','es','gg','gj','mj','sd']:
        count += 1
        sql = 'select * from %s_traffic_byday '%s
        df = execute_sql(sql)
        df.set_index(['date'],inplace=True)
        if count == 1:
            std_date = list(df.index)
            all_df = df
        else:
            date = list(df.index)
            users = list(df['users'])
            newUsers = list(df['newUsers'])
            sessions = list(df['sessions'])

            for i in std_date:
                if i not in date:
                    date.insert(std_date.index(i),i)
                    users.insert(std_date.index(i),0)
                    newUsers.insert(std_date.index(i),0)
                    sessions.insert(std_date.index(i),0)
            df = pd.DataFrame(zip(date,users,newUsers,sessions),columns=['date','users','newUsers','sessions'])
            #把前一个值填到缺失值上
            df.fillna(method='ffill',inplace=True)
            df.set_index(['date'],inplace=True)
            all_df += df
    all_df['newUsers'] = all_df['newUsers'].astype('int64')
    allsite_users_byday = list(all_df['users'])
    allsite_newUsers_byday = list(all_df['newUsers'])
    allsite_sessions_byday = list(all_df['sessions'])
    allsite_date_byday = list(all_df.index)

    
    

    return allsite_users_byday,allsite_newUsers_byday,allsite_sessions_byday,allsite_date_byday
#获取全部网站销售额
def get_allsite_sales(start_time=start_time,end_time=end_time):
    start_timeArray = time.localtime(start_time)
    end_timeArray = time.localtime(end_time)
    begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
    finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
    count = 0
    for i in ['es','gg','gj','mj','sd']:
        count += 1
        sales_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,sum(money) as sales from %s_order where add_time between %d and %d and flid in (2,3,4,5,9,10) group by month order by month'%(i,start_time,end_time)
        df = execute_sql(sales_sql)
        df.set_index('month',inplace =True)
        if count == 1:
            sum_df = df
        else:
            sum_df += df
    bs_sales_sql = "select substr(add_time,1,7) as month,sum(paid_money) as sales from bs_order where add_time between '%s' and '%s' group by month order by month"%(begin_time,finish_time)
    bs_df = execute_sql(bs_sales_sql)
    bs_df.set_index('month',inplace = True)
    allsite_sales_df = sum_df+bs_df
    allsite_sales_df['month'] = allsite_sales_df.index
    order = ['month','sales']
    allsite_sales_df = allsite_sales_df[order]
    sales_list = allsite_sales_df.values.tolist()
    allsite_sales = [order]
    allsite_sales.extend(sales_list)
    for i in allsite_sales[1:]:
         i[1] = round(i[1],0)

    
    return allsite_sales

#获取全部网站GMV，并以月份为第一维度，网站为第二维度
def get_allsite_GMV(start_time=start_time,end_time=end_time):
    start_timeArray = time.localtime(start_time)
    end_timeArray = time.localtime(end_time)
    begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
    finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
    count = 0
    for i in ['es','gg','gj','mj','sd']:
        count += 1
        GMV_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,sum(money) as GMV from %s_order where add_time between %d and %d group by month order by month'%(i,start_time,end_time)
        df = execute_sql(GMV_sql)
        
        df.set_index('month',inplace =True)
        if count == 1:
            es_GMV = [int(x[0].round(0)) for x in df.values]
           
        if count == 2:
            gg_GMV = [int(x[0].round(0)) for x in df.values]
           
        if count == 3:
            gj_GMV = [int(x[0].round(0)) for x in df.values]

        if count == 4:
           
            mj_GMV = [int(x[0].round(0)) for x in df.values]
        else:
            sd_GMV = [int(x[0].round(0)) for x in df.values]
    bs_GMV_sql = "select substr(add_time,1,7) as month,sum(paid_money) as GMV from bs_order where add_time between '%s' and '%s' group by month order by month"%(begin_time,finish_time)
    bs_df = execute_sql(bs_GMV_sql)
    bs_df.set_index('month',inplace =True)
    bs_GMV = [int(x[0].round(0)) for x in bs_df.values]
    month_list = [m for m in  bs_df.index]
    

    return es_GMV,gg_GMV,gj_GMV,mj_GMV,sd_GMV,bs_GMV,month_list

#获取全部网站的总订单和支付成功订单，以月分为维度，并计算各月支付成功率
def get_allsite_orders(start_time=start_time,end_time=end_time):
    start_timeArray = time.localtime(start_time)
    end_timeArray = time.localtime(end_time)
    begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
    finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
    count = 0
    for i in ['es','gg','gj','mj','sd']:
        count += 1
        sql_all_order = 'select substr(FROM_unixtime(add_time),1,7) as month,count(orderid) as all_orders from %s_order where add_time between %d and %d group by month order by month'%(i,start_time,end_time)
        all_order_df = execute_sql(sql_all_order)
        all_order_df.set_index('month',inplace = True)
        sql_paid_order =  'select substr(FROM_unixtime(add_time),1,7) as month,count(orderid) as paid_orders from %s_order where add_time between %d and %d and flid in (2,3,4,5,9,10) group by month order by month'%(i,start_time,end_time)
        paid_order_df = execute_sql(sql_paid_order)
        paid_order_df.set_index('month',inplace = True)
        if count ==1:
            part_site_order = all_order_df
            part_site_paid_order = paid_order_df
        else:
            part_site_order += all_order_df
            part_site_paid_order += paid_order_df
    bs_all_order_sql = "select substr(add_time,1,7) as month,count(orderid) as all_orders from bs_order where add_time between '%s' and '%s' group by month order by month"%(begin_time,finish_time)
    bs_paid_order_sql  = "select substr(add_time,1,7) as month,count(orderid) as paid_orders from bs_order where add_time between '%s' and '%s' and flid in (2,3,4,5,23) group by month order by month"%(begin_time,finish_time)
    bs_all_order_df = execute_sql(bs_all_order_sql).set_index('month')
    bs_paid_order_df = execute_sql(bs_paid_order_sql).set_index('month')
    all_site_order = [int(x) for x in (part_site_order+bs_all_order_df).values]
    all_site_paid_order = [int(y) for y in (part_site_paid_order+bs_paid_order_df).values]
    origin_rate = list(np.array(all_site_paid_order)/np.array(all_site_order))
    paid_rate = []
    #支付成功率保留2位小数
    for r in origin_rate:
        paid_rate.append(round(r,2))

    # paid_rate = ['%.1f%%'%(r*100) for r in rate]
    order_month  = [m for m in bs_all_order_df.index]

    return all_site_order,all_site_paid_order,paid_rate,order_month

#获取全部网站的流量来源数据，月份为第一维度，来源为第二维度
def get_allsite_traffic_source():
    count = 0
    for i in ['es','gg','gj','mj','sd','bs']: 
        count += 1
        sql = "select month,`group`,users from %s_traffic_source_by_month "%(i)
        df = execute_sql(sql)
        if count == 1:
            apd_df = df
        else:
            apd_df = apd_df.append(df)
    #创建透视表可以更方便的获取前端需要的数据结构
    table = apd_df.pivot_table(columns = ['month'],index =['group'],values = ['users'],aggfunc = np.sum )
    first_line =  [x for x in table.columns.levels[1]]  
    first_line.insert(0,'group')
    dataset = []
    dataset.append(first_line)
    value_list = table.values.tolist()
    for i in value_list:
        i.insert(0,[j for j in table.index][value_list.index(i)])
        dataset.append(i)

    return dataset

def get_allsite_reg_rate(start_time=start_time,end_time=end_time):
    start_timeArray = time.localtime(start_time)
    end_timeArray = time.localtime(end_time)
    begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
    finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
    #从各网站流量表获取新用户数，将结果(dataframe)求和
    count = 0
    for i in ['es','gg','gj','mj','sd','bs']: 
        count += 1
        newUser_sql = 'select month,newUsers from %s_traffic_by_month order by month'%i
        df = execute_sql(newUser_sql)
        df.set_index('month',inplace = True)
        df['newUsers'] = df['newUsers'].astype('int')
        if count == 1:
            newUsers_df = df
        else:
            newUsers_df += df

    cnt = 0
    for x in ['es','gg','gj','mj','sd']:
        cnt += 1
        reg_sql = 'select substr(FROM_UNIXTIME(reg_time),1,7) as month,count(uid) as reg_users from %s_customer where reg_time between %d and %d group by month order by month '%(x,start_time,end_time)
        part_reg_df = execute_sql(reg_sql)
        part_reg_df.set_index('month',inplace = True)
        if cnt == 1:
            part_df = part_reg_df
        else:
            part_df += part_reg_df
    bs_sql = "select substr(reg_time,1,7) as month,count(uid) as reg_users from bs_customer where reg_time between '%s' and '%s' group by month"%(begin_time,finish_time)
    bs_df = execute_sql(bs_sql)
    bs_df.set_index('month',inplace = True)
    reg_df = part_df+bs_df
    new_users = [int(n) for n in newUsers_df.values]
    reg_users = [int(c) for c in reg_df.values]
    reg_rate = list(np.array(reg_users)/np.array(new_users))
    reg_rate = list(map(lambda x:round(x,3),reg_rate))
    reg_month = [m for m in bs_df.index]

    return new_users,reg_users,reg_rate,reg_month

#获取转化率
def get_allsite_conversion_rate(start_time=start_time,end_time=end_time):
    start_timeArray = time.localtime(start_time)
    end_timeArray = time.localtime(end_time)
    begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
    finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
    cnt = 0
    for i in ['es','gg','gj','mj','sd']:
        cnt += 1
        order_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,count(orderid) as all_orders from %s_order where add_time between %d and %d group by month order by month'%(i,start_time,end_time)
        paid_order_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,count(orderid) as paid_orders from %s_order where add_time between %d and %d and flid in (2,3,4,5,9,10) group by month order by month'%(i,start_time,end_time)
        order_df  = execute_sql(order_sql)
        order_df.set_index('month',inplace=True)
        paid_df = execute_sql(paid_order_sql)
        paid_df.set_index('month',inplace=True)
        if cnt == 1:
            all_order_df = order_df
            paid_order_df = paid_df
        else:
            all_order_df += order_df
            paid_order_df += paid_df
    bs_order_sql = "select substr(add_time,1,7) as month,count(orderid) as all_orders from bs_order where add_time between '%s' and '%s' group by month order by month"%(begin_time,finish_time)
    bs_paid_order_sql  = "select substr(add_time,1,7) as month,count(orderid) as paid_orders from bs_order where add_time between '%s' and '%s' and flid in (2,3,4,5,23) group by month order by month"%(begin_time,finish_time)
    bs_order_df = execute_sql(bs_order_sql)
    bs_order_df.set_index('month',inplace=True)
    bs_paid_order_df = execute_sql(bs_paid_order_sql)
    bs_paid_order_df.set_index('month',inplace=True)
    allsite_order_df = all_order_df+bs_order_df
    allsite_paid_order_df = paid_order_df+bs_paid_order_df
    
    count =0 
    for s in ['es','gg','gj','mj','sd','bs']:
        count += 1
        visitor_sql = "select month,users from %s_traffic_by_month order by month "%s
        df = execute_sql(visitor_sql)
        df.set_index('month',inplace=True)
        df['users']=df['users'].astype('int')
        if count ==1:
            user_df = df
        else:
            user_df += df
    convert_month = [m for m in user_df.index]
    visitor_list = [int(v) for v in user_df.values]
    allsite_order_list = [int(y) for y in allsite_order_df.values]
    allsite_paid_order_list = [int(z) for z in allsite_paid_order_df.values]
    convert_rate_list = list(map(lambda x:'{:.1%}'.format(x),list(np.array(allsite_paid_order_list)/np.array(visitor_list))))
 
    return convert_month,visitor_list,allsite_order_list,allsite_paid_order_list,convert_rate_list
    
   
#获取全部网站活跃度
def get_allsite_acitivation():
    #从各网站流量表获取用户数，将结果(dataframe)求和
    count = 0
    for i in ['es','gg','gj','mj','sd','bs']:
        count += 1
        sql1 = 'select month,users from %s_traffic_by_month order by month'%i
        sql2 = 'select month,users as activate_uers from %s_activeusers_by_month order by month'%i
        df1 = execute_sql(sql1)
        df2 = execute_sql(sql2)
        df1.set_index('month',inplace = True)
        df2.set_index('month',inplace = True)
        df1['users']=df1['users'].astype('int')
        df2['activate_uers']=df2['activate_uers'].astype('int')
        if count == 1:
            users_df = df1
            active_users_df = df2
        else:
            users_df += df1
            active_users_df += df2
    allsite_users  = [int(x) for x in users_df.values]
    allsite_active_users = [int(y) for y in active_users_df.values]
    active_rate = list(np.array(allsite_active_users)/np.array(allsite_users))
    allsite_active_rate = list(map(lambda x:round(x,3),active_rate))
    allsite_active_month = [m for m in users_df.index]

    return allsite_users,allsite_active_users,allsite_active_rate,allsite_active_month


#获取上个月的Top30搜索关键词
def get_search_term():
    word_list = []
    word_dict = {}
    search_records =0
    monlist = [['20190101','20190131'],['20190201','20190228'],['20190301','20190331'],['20190401','20190430'],['20190501','20190531'],
['20190601','20190630'],['20190701','20190731'],['20190801','20190831'],['20190901','20190930'],['20191001','20191031']
,['20191101','20191130'],['20191201','20191231'],['20200101','20200131'],['20200201','20200229'],['20200301','20200331'],
['20200401','20200430'],['20200501','20200531'],['20200601','20200630']]
    now = time.localtime(time.time())
    i = monlist[now.tm_mon+(now.tm_year-2019)*12-2]

    for j in ['es','gj','mj','sd','bs']:
        sql = "select date,term,search_times from %s_search_term_by_day where date between '%s' and '%s'"%(j,i[0],i[1])
        df = execute_sql(sql)
        df['term'] = pd.Series(map(lambda x:x+' ',df['term']))
        df2 = df['term']*df['search_times']
        search_records += np.sum(df['search_times'])
        for w in df2:
            #将搜索的内容全部拆分为单个词
            for v in w.split(' '):
                #判断字符串是否是空格
                if v.isspace()== False:
                    #判断字符串是否是英文字母，是就全部转小写
                    if v.isalpha()==True:
                        word_list.append(v.lower())
                    else:
                        word_list.append(v)   
    for word in word_list:
        if word not in word_dict:
            word_dict[word]=1
        else:
            word_dict[word]+=1
    #依据关键词频率从高到低排序
    order_dict = OrderedDict(sorted(word_dict.items(),key = lambda x:x[1],reverse = True))
    #删除无意义的关键词
    
    order_dict.pop('')
    order_dict.pop('jersey')
    order_dict.pop('real')
    order_dict.pop('soccer')
    order_dict.pop('jerseys')
    order_dict.pop('city')
    order_dict.pop('long')
    #取前三十排名的关键词
    count_list = list(order_dict.items())[0:30]
    #将各关键词对应的统计频次、搜索率存放在不同的列表
    search_term = []
    search_times = []
    search_rate = []
    for s in count_list:
        search_term.append(s[0])
        search_times.append(s[1])
        search_rate.append(round(s[1]/search_records,3)) 

    return search_term,search_times,search_rate

#get_paidorder_device函数返回的二维列表中的数值是numpy.int64格式，直接转json会报错。需要转换成python Native int格式
def Convert_into_int(x):
    list1 = []
    for i in x:
        list2 = []
        for j in i:
            if not isinstance(j,np.int64):#str格式的数据不管，保持原样就行
                list2.append(j)
            else:
                j = j.item()#item（）可以将numpy.int64格式转换成int
                list2.append(j)
        list1.append(list2)

    return list1
    

class Single_site_data():
    DB = None
    def __init__(self, prefix):
        self.prefix = prefix

#获取单个网站的每日用户数、新用户数、会话数 
    def get_uns(self,start_time=start_time,end_time=end_time):    
        
        bs_sql = 'select * from bs_traffic_byday '
        bs_df = execute_sql(bs_sql)
        std_date = list(bs_df['date'])
        sql = 'select * from %s_traffic_byday '%self.prefix
        df = execute_sql(sql)
        df.set_index(['date'],inplace=True)    
        date = list(df.index)
        users = list(df['users'])
        newUsers = list(df['newUsers'])
        sessions = list(df['sessions'])

        for i in std_date:
            if i not in date:
                date.insert(std_date.index(i),i)
                users.insert(std_date.index(i),0)
                newUsers.insert(std_date.index(i),0)
                sessions.insert(std_date.index(i),0)
        df = pd.DataFrame(zip(date,users,newUsers,sessions),columns=['date','users','newUsers','sessions'])
        #把前一个值填到缺失值上
        df.fillna(method='ffill',inplace=True)
        df.set_index(['date'],inplace=True)
        df['newUsers'] = df['newUsers'].astype('int64')
        
        users_byday = list(df['users'])
        newUsers_byday = list(df['newUsers'])
        sessions_byday = list(df['sessions'])
        date_byday = list(df.index)
 
    
        return users_byday,newUsers_byday,sessions_byday,date_byday

    def get_conversion_rate(self,start_time=start_time,end_time=end_time):
        if self.prefix =='bs':
            start_timeArray = time.localtime(start_time)
            end_timeArray = time.localtime(end_time)
            begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
            finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
            order_sql = "select substr(add_time,1,7) as month,count(orderid) as all_orders from bs_order where add_time between '%s' and '%s' group by month order by month"%(begin_time,finish_time)
            paid_order_sql  = "select substr(add_time,1,7) as month,count(orderid) as paid_orders from bs_order where add_time between '%s' and '%s' and flid in (2,3,4,5,23) group by month order by month"%(begin_time,finish_time)
            order_df = execute_sql(order_sql)
            order_df.set_index('month',inplace=True)
            paid_order_df = execute_sql(paid_order_sql)
            paid_order_df.set_index('month',inplace=True)
            visitor_sql = "select month,users from %s_traffic_by_month order by month "%self.prefix
            df = execute_sql(visitor_sql)
            df.set_index('month',inplace=True)
            df['users']=df['users'].astype('int')
            single_convert_month = [m for m in df.index]
            single_visitor_list = [int(v) for v in df.values]
            single_order_list = [int(y) for y in order_df.values]
            single_paid_order_list = [int(z) for z in paid_order_df.values]
            single_convert_rate_list = list(map(lambda x:'{:.1%}'.format(x),list(np.array(single_paid_order_list)/np.array(single_visitor_list))))
        else:
            order_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,count(orderid) as all_orders from %s_order where add_time between %d and %d group by month order by month'%(self.prefix,start_time,end_time)
            paid_order_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,count(orderid) as paid_orders from %s_order where add_time between %d and %d and flid in (2,3,4,5,9,10) group by month order by month'%(self.prefix,start_time,end_time)
            order_df  = execute_sql(order_sql)
            order_df.set_index('month',inplace=True)
            paid_df = execute_sql(paid_order_sql)
            paid_df.set_index('month',inplace=True)
            visitor_sql = "select month,users from %s_traffic_by_month order by month "%self.prefix
            df = execute_sql(visitor_sql)
            df.set_index('month',inplace=True)
            df['users']=df['users'].astype('int')
            single_convert_month = [m for m in df.index]
            single_visitor_list = [int(v) for v in df.values]
            single_order_list = [int(y) for y in order_df.values]
            single_paid_order_list = [int(z) for z in paid_df.values]
            single_convert_rate_list = list(map(lambda x:'{:.1%}'.format(x),list(np.array(single_paid_order_list)/np.array(single_visitor_list))))
        
        return single_convert_month,single_visitor_list,single_order_list,single_paid_order_list,single_convert_rate_list
            

    #获取GMV和sale实现GMV和sale的柱状图，月份为横轴
    def get_sales(self,start_time=start_time,end_time=end_time):
        if self.prefix =='bs':
            start_timeArray = time.localtime(start_time)
            end_timeArray = time.localtime(end_time)
            begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
            finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
            sales_sql = "select substr(add_time,1,7) as month,sum(paid_money) as sales from %s_order where add_time between '%s' and '%s' and flid in (2,3,4,5,9,10) group by month order by month"%(self.prefix,begin_time,finish_time)            
            sales_df = execute_sql(sales_sql)
            sales_df.set_index('month',inplace=True)
            sales_month_list = [ x for x in sales_df.index]
            sales  = [int(v)  for v in sales_df.values]
        else:
            sales_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,sum(money) as sales from %s_order where add_time between %d and %d and flid in (2,3,4,5,9,10) group by month order by month'%(self.prefix,start_time,end_time)
            sales_df = execute_sql(sales_sql)
            sales_df.set_index('month',inplace=True)
            sales_month_list = [ x for x in sales_df.index]
            sales  = [int(v)  for v in sales_df.values]
        return sales_month_list,sales
    def get_GMV(self,start_time=start_time,end_time=end_time):
        if self.prefix =='bs':
            start_timeArray = time.localtime(start_time)
            end_timeArray = time.localtime(end_time)
            begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
            finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
            GMV_sql = "select substr(add_time,1,7) as month,sum(money) as GMV from %s_order where add_time between '%s' and '%s' group by month order by month"%(self.prefix,begin_time,finish_time)
            GMV_df = execute_sql(GMV_sql)
            GMV_df.set_index('month',inplace=True)
            GMV = [int(g) for g in GMV_df.values]
        else:
            GMV_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,sum(money) as GMV from %s_order where add_time between %d and %d group by month order by month'%(self.prefix,start_time,end_time)
            GMV_df = execute_sql(GMV_sql)
            GMV_df.set_index('month',inplace=True)
            GMV = [int(g) for g in GMV_df.values]
        return GMV
    #获取总订单、支付成功订单、计算支付成功率(粒度为月)
    def get_order(self,start_time=start_time,end_time=end_time):
        if self.prefix =='bs':
            start_timeArray = time.localtime(start_time)
            end_timeArray = time.localtime(end_time)
            begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
            finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
            order_sql = "select substr(add_time,1,7) as month,count(orderid) as all_orders from %s_order where add_time between '%s' and '%s' group by month order by month"%(self.prefix,begin_time,finish_time)
            order_df = execute_sql(order_sql)
            order_df.set_index('month',inplace=True)
            paid_order_sql = "select substr(add_time,1,7) as month,count(orderid) as paid_orders from %s_order where add_time between '%s' and '%s' and flid in (2,3,4,5,9,10) group by month order by month"%(self.prefix,begin_time,finish_time)
            paid_order_df = execute_sql(paid_order_sql)
            paid_order_df.set_index('month',inplace=True)
            order = [int(i) for i in order_df.values]
            paid_order = [int(j) for j in paid_order_df.values]
            rate = list(np.array(paid_order)/np.array(order))
            paid_rate = list(map(lambda x:round(x,2),rate))
        else: 
            order_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,count(orderid) as all_orders from %s_order where add_time between %d and %d group by month order by month'%(self.prefix,start_time,end_time)
            order_df = execute_sql(order_sql)
            order_df.set_index('month',inplace=True)
            paid_order_sql = 'select substr(FROM_unixtime(add_time),1,7) as month,count(orderid) as paid_orders from %s_order where add_time between %d and %d and flid in (2,3,4,5,9,10) group by month order by month'%(self.prefix,start_time,end_time)
            paid_order_df = execute_sql(paid_order_sql)
            paid_order_df.set_index('month',inplace=True)
            order = [int(i) for i in order_df.values]
            paid_order = [int(j) for j in paid_order_df.values]
            rate = list(np.array(paid_order)/np.array(order))
            paid_rate = list(map(lambda x:round(x,2),rate))
        return paid_order,order,paid_rate
    #获取流量来源
    def get_traffic_source(self): 
        source_sql = '''select month,`group`,users from %s_traffic_source_by_month where month like '2019%%' ''' %(self.prefix)
        source_df = execute_sql(source_sql)
        table = source_df.pivot_table(index=['group'],columns=['month'],values=['users'],aggfunc=np.sum)
        first_line =  [x for x in table.columns.levels[1]]   
        first_line.insert(0,'group')
        source_dataset = []
        source_dataset.append(first_line)
        value_list = table.values.tolist()
        for i in value_list:
            i.insert(0,[j for j in table.index][value_list.index(i)])
            source_dataset.append(i)
        return source_dataset
    #获取新访问用户、注册用户、注册率
    def get_register_rate(self,start_time=start_time,end_time=end_time):
        if self.prefix == 'bs':
            start_timeArray = time.localtime(start_time)
            end_timeArray = time.localtime(end_time)
            begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
            finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
            new_Users_sql = 'select month,newUsers from %s_traffic_by_month order by month'%self.prefix
            new_Users_df = execute_sql(new_Users_sql)
            new_Users_df.set_index('month',inplace=True)
            new_Users = [int(n) for n in new_Users_df.values]
            register_user_sql = "select substr(reg_time,1,7) as month,count(uid) as reg_users from %s_customer where reg_time between '%s' and '%s' group by month order by month "%(self.prefix,begin_time,finish_time)
            register_user_df = execute_sql(register_user_sql)
            register_user_df.set_index('month',inplace=True)
            register_users = [int(r) for r in register_user_df.values]
            rate = list(np.array(register_users)/np.array(new_Users))
            register_rate = list(map(lambda x:round(x,2),rate))
            register_month  = [m for m in new_Users_df.index]
        else:
            new_Users_sql = 'select month,newUsers from %s_traffic_by_month order by month'%self.prefix
            new_Users_df = execute_sql(new_Users_sql)
            new_Users_df.set_index('month',inplace=True)
            new_Users = [int(n) for n in new_Users_df.values]
            register_user_sql = 'select substr(FROM_UNIXTIME(reg_time),1,7) as month,count(uid) as reg_users from %s_customer where reg_time between %d and %d group by month order by month '%(self.prefix,start_time,end_time)
            register_user_df = execute_sql(register_user_sql)
            register_user_df.set_index('month',inplace=True)
            register_users = [int(r) for r in register_user_df.values]
            rate = list(np.array(register_users)/np.array(new_Users))
            register_rate = list(map(lambda x:round(x,2),rate))
            register_month  = [m for m in new_Users_df.index]
        return new_Users,register_users,register_rate,register_month
    #获取用户数、活跃用户数、活跃率、
    def get_activation(self):
        users_sql = 'select month,users from %s_traffic_by_month order by month'%self.prefix
        users_df = execute_sql(users_sql)
        users_df.set_index('month',inplace=True)
        activate_users_sql = 'select month,users as activate_uers from %s_activeusers_by_month order by month'%self.prefix
        activate_users_df = execute_sql(activate_users_sql)
        activate_users_df.set_index('month',inplace=True)
        users = [int(u) for u in users_df.values]
        activate_users = [int(a) for a in activate_users_df.values]
        rate = list(np.array(activate_users)/np.array(users))
        activate_rate = list(map(lambda r:round(r,2),rate))
        activate_month = [m for m in users_df.index]
        return users,activate_users,activate_rate,activate_month
    #获取各设备支付成功订单&支付成功率
    def get_paidorder_bydevice(self,start_time=start_time,end_time=end_time):
        if self.prefix =='bs':
            start_timeArray = time.localtime(start_time)
            end_timeArray = time.localtime(end_time)
            begin_time = time.strftime('%Y-%m-%d %H:%M:%S',start_timeArray)
            finish_time = time.strftime('%Y-%m-%d %H:%M:%S',end_timeArray)
            order_sql = "SELECT SUBSTR(add_time,1,7) AS month,count(CASE WHEN device = 0 THEN orderid  END) AS PC,COUNT(CASE WHEN device = 1 THEN orderid END) AS Mobile,COUNT(CASE WHEN device = 2 THEN orderid END) AS Tablet FROM %s_order WHERE add_time BETWEEN '%s' AND '%s' GROUP BY month"%(self.prefix,begin_time,finish_time)
            paid_order_sql = "SELECT SUBSTR(add_time,1,7) AS month,count(CASE WHEN device = 0 THEN orderid  END) AS PC,COUNT(CASE WHEN device = 1 THEN orderid END) AS Mobile,COUNT(CASE WHEN device = 2 THEN orderid END) AS Tablet FROM %s_order WHERE add_time BETWEEN '%s' AND '%s' AND flid IN (2,3,4,5,9,10) GROUP BY month"%(self.prefix,begin_time,finish_time)
            order_df = execute_sql(order_sql)
            order_df[['PC','Mobile','Tablet']] = order_df[['PC','Mobile','Tablet']].astype('int')
            order_df.set_index('month',inplace=True)
            paid_order_df = execute_sql(paid_order_sql)
            paid_order_df.set_index('month',inplace=True)
            rate_df = paid_order_df/order_df
            rate_df.fillna(0,inplace=True) 
            #把array转化成二维列表
            l1 = list(rate_df.values)
            l2 = map(lambda x:list(map(lambda y:round(y,2),x)),l1)
            rate_dataset = [ i for i in l2]    
            rate_dataset.insert(0,list(rate_df.columns))
            l3 = list(paid_order_df.index)
            l3.insert(0,'month')
            for m in range(len(rate_dataset)):
                rate_dataset[m].insert(0,l3[m])
            l4 = list(paid_order_df.values)
            l5 = map(lambda x:list(x),l4)
            paid_order_dataset = [i for i in l5]
            paid_order_dataset.insert(0,list(paid_order_df.columns))
            for m in range(len(paid_order_dataset)):
                paid_order_dataset[m].insert(0,l3[m])
            paid_order_dataset = Convert_into_int(paid_order_dataset)
            
        else:
            order_sql = "SELECT SUBSTR(FROM_UNIXTIME(add_time),1,7) AS month,count(CASE WHEN device = 0 THEN orderid  END) AS PC,COUNT(CASE WHEN device = 1 THEN orderid END) AS Mobile,COUNT(CASE WHEN device = 2 THEN orderid END) AS Tablet FROM %s_order WHERE add_time BETWEEN %d AND %d GROUP BY month"%(self.prefix,start_time,end_time)
            paid_order_sql = "SELECT SUBSTR(FROM_UNIXTIME(add_time),1,7) AS month,count(CASE WHEN device = 0 THEN orderid  END) AS PC,COUNT(CASE WHEN device = 1 THEN orderid END) AS Mobile,COUNT(CASE WHEN device = 2 THEN orderid END) AS Tablet FROM %s_order WHERE add_time BETWEEN %d AND %d AND flid IN (2,3,4,5,9,10) GROUP BY month"%(self.prefix,start_time,end_time)
            order_df = execute_sql(order_sql)
            order_df[['PC','Mobile','Tablet']] = order_df[['PC','Mobile','Tablet']].astype('int')
            order_df.set_index('month',inplace=True)
            paid_order_df = execute_sql(paid_order_sql)
            paid_order_df.set_index('month',inplace=True) 
            rate_df = paid_order_df/order_df
            rate_df.fillna(0,inplace=True)
            #把array转化成二维列表
            l1 = list(rate_df.values)
            l2 = map(lambda x:list(map(lambda y:round(y,2),x)),l1)
            rate_dataset = [ i for i in l2]    
            rate_dataset.insert(0,list(rate_df.columns))
            l3 = list(paid_order_df.index)
            l3.insert(0,'month')
            for m in range(len(rate_dataset)):
                rate_dataset[m].insert(0,l3[m])
            l4 = list(paid_order_df.values)
            l5 = map(lambda x:list(x),l4)
            paid_order_dataset = [i for i in l5]
            paid_order_dataset.insert(0,list(paid_order_df.columns))
            for m in range(len(paid_order_dataset)):
                paid_order_dataset[m].insert(0,l3[m])
            paid_order_dataset = Convert_into_int(paid_order_dataset)
        return rate_dataset,paid_order_dataset