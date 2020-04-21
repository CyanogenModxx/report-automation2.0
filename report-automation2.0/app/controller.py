import pandas as pd 
import numpy as np 
import pymysql
from sqlalchemy import create_engine
import datetime
import time
from config import execute_sql



class Data_monitoring(object):
    def __init__(self, prefix):
        self.prefix = prefix
    @staticmethod
    def get_time():
        today = datetime.date.today()
    
        #昨天 XXXX-XX-XX 
        yestoday_str = today.__format__('%Y-%m-%d') + ' 00:00:00'
        yestoday_timeArray = time.strptime(yestoday_str, "%Y-%m-%d %H:%M:%S")
        #昨天 20191111
        yestoday_ga = (today-datetime.timedelta(1)).__format__('%Y%m%d')
        #昨天结束的时间戳
        yestoday  = int(time.mktime(yestoday_timeArray))
        #过去30天的第一天 XXXX-XX-XX
        fst_day_str = (today-datetime.timedelta(30)).__format__('%Y-%m-%d') + ' 00:00:00'
        #过去30天的第一天 20191031
        fst_day_ga = (today-datetime.timedelta(30)).__format__('%Y%m%d')
        #过去30天的第一天的前一天：针对GA时间格式 
        fst_day_ga_tail = (today-datetime.timedelta(31)).__format__('%Y%m%d')
        #过去30天的第一天时间戳
        fst_day_timeArray = time.strptime(fst_day_str,"%Y-%m-%d %H:%M:%S")
        fst_day = int(time.mktime(fst_day_timeArray))
    
        time_interval = [fst_day_str,yestoday_str,fst_day,yestoday,fst_day_ga,yestoday_ga,fst_day_ga_tail]
        
        return time_interval

    #获取标准日期，因为在GA统计的数据中，如果某天的数据为0，那么该日期就不会存在，那么日期不一样的Dataframe不能相加。
    @staticmethod
    def get_standard_date():
        today = datetime.date.today()
        history_std_date = []
        current_std_date= []
        first_day = datetime.date(2019,1,1)
        final_day  = today-datetime.timedelta(31)
        while final_day>=first_day:
            history_std_date.append(first_day.__format__('%Y-%m-%d'))
            first_day += datetime.timedelta(1)

        current_month_fstday = today-datetime.timedelta(30)
        current_month_yestoday = today-datetime.timedelta(1)
        while current_month_yestoday>=current_month_fstday:
            current_std_date.append(current_month_fstday.__format__('%Y-%m-%d'))
            current_month_fstday += datetime.timedelta(1)
        
        return history_std_date,current_std_date
    
    @staticmethod
    def get_GA_standard_date():
        today = datetime.date.today()
        ga_history_std_date = []
        ga_current_std_date= []
        first_day = datetime.date(2019,1,1)
        final_day  = today-datetime.timedelta(31)
        while final_day>=first_day:
            ga_history_std_date.append(first_day.__format__('%Y%m%d'))
            first_day += datetime.timedelta(1)

        current_month_fstday = today-datetime.timedelta(30)
        current_month_yestoday = today-datetime.timedelta(1)
        while current_month_yestoday>=current_month_fstday:
            ga_current_std_date.append(current_month_fstday.__format__('%Y%m%d'))
            current_month_fstday += datetime.timedelta(1)
        
        return ga_history_std_date,ga_current_std_date

    # #通用函数
    # def general_func(self,history_sql:str,current_sql:str,column_A:str,collumn_B:str,sites:list):
    #     history_std_date,current_std_date = self.get_standard_date()
    #     cnt=0
    #     for i in sites:
    #         cnt+=1
    #         history_df = execute_sql(history_sql)
    #         current_df = execute_sql(current_sql)
    #         history_date = list(history_df[column_A])
    #         history_value = list(history_df[collumn_B])
    #         current_date = list(current_df[column_A])
    #         current_value = list(current_df[collumn_B])
    #         for i in history_std_date:
    #             if i not in history_date:
    #                 history_date.insert(history_std_date.index(i),i)
    #                 history_value.insert(history_std_date.index(i),0)
    #         new_history_df = pd.DataFrame(zip(history_date,history_value),columns=[column_A,collumn_B])
    #         new_history_df.set_index([column_A],inplace=True)

    #         for j in current_std_date:
    #             if j not in current_date:
    #                 current_date.insert(current_std_date.index(j),j)
    #                 current_value.insert(current_std_date.index(j),0)
    #         new_current_df = pd.DataFrame(zip(current_date,current_value),columns=[column_A,collumn_B])
    #         new_current_df.set_index([column_A],inplace=True)

    #         if cnt == 1:
    #             combined_history_df = new_history_df
    #             combined_current_df = new_current_df
    #         else:
    #             combined_history_df += new_history_df
    #             combined_current_df += new_current_df 

    #     return combined_history_df,combined_current_df 

            
        

    #获取当月每天的用户数以及预警值。预警值 = 历月平均数-标准差
    def get_users(self):
        ga_history_std_date,ga_current_std_date= self.get_GA_standard_date()
        if self.prefix == 'allsite':
            cnt = 0
            for i in ['es','gg','gj','mj','sd','bs']:
                cnt += 1
                sql_history = "select date,users from %s_traffic_byday where date between '20190101' and '%s' "%(i,self.get_time()[6])
                sql = "select date,users from %s_traffic_byday where date between '%s' and '%s' "%(i,self.get_time()[4],self.get_time()[5])
                df = execute_sql(sql_history)
                current_df = execute_sql(sql)
                date = list(df['date'])
                value = list(df['users'])
                current_date = list(current_df['date'])
                current_value = list(current_df['users'])
                for i in ga_history_std_date:
                    if i not in date:
                        date.insert(ga_history_std_date.index(i),i)
                        value.insert(ga_history_std_date.index(i),0)
                for n in ga_current_std_date:
                    if n not in current_date:
                        current_date.insert(ga_current_std_date.index(n),n)
                        current_value.insert(ga_current_std_date.index(n),0)

                df = pd.DataFrame(zip(date,value),columns=['date','users'])
                current_df = pd.DataFrame(zip(current_date,current_value),columns=['date','users'])
                df.set_index(['date'],inplace=True)
                current_df.set_index(['date'],inplace=True)
                if cnt ==1:
                    all_df = df
                    combined_df = current_df
                else:
                    all_df += df
                    combined_df += current_df

            user_alert_value = all_df['users'].mean()-all_df['users'].std()
            user_avg = all_df['users'].mean()
            user_date = list(combined_df.index)
            user = list(combined_df['users'])
        else:
            sql_history = "select date,users from %s_traffic_byday where date between '20190101' and '%s' "%(self.prefix,self.get_time()[6])
            df = execute_sql(sql_history)
            user_history = list(df['users'])
            history_date = list(df['date'])
            for i in ga_history_std_date:
                if i not in history_date:
                    user_history.insert(ga_history_std_date.index(i),0)
            
            user_alert_value = (np.array(user_history)).mean() - (np.array(user_history)).std()
            user_avg = (np.array(user_history)).mean()
            sql = "select date,users from %s_traffic_byday where date between '%s' and '%s' "%(self.prefix,self.get_time()[4],self.get_time()[5])
            user_df = execute_sql(sql)
            user_date  = list(user_df['date'])
            user = list(user_df['users'])
            for n in ga_current_std_date:
                if n not in user_date:
                    user_date.insert(ga_current_std_date.index(n),n)
                    user.insert(ga_current_std_date.index(n),0)
        
        
        return user_avg,user_alert_value,user_date,user


    #获取当月每天的注册用户数以及预警值。预警值 = 历月平均数-2*标准差
    def sign_up_user(self):
        history_std_date,current_std_date= self.get_standard_date()
        if self.prefix == 'allsite':
            sites = ['es','gg','gj','mj','sd']

            cnt = 0
            for i in ['es','gg','gj','mj','sd']:
                cnt += 1
                sql_history = "select substr(from_unixtime(reg_time),1,10) as date,count(uid) as reg_users from %s_customer where reg_time between 1546272000 and %d group by date order by date "%(i,self.get_time()[2])
                sql_current = "select substr(from_unixtime(reg_time),1,10) as date,count(uid) as reg_users from %s_customer where reg_time between %d and %d group by date order by date"%(i,self.get_time()[2],self.get_time()[3])
                df_history = execute_sql(sql_history)
                df_current = execute_sql(sql_current)
                date_history = list(df_history['date'])
                value_history = list(df_history['reg_users'])
                date_current = list(df_current['date'])
                value_current = list(df_current['reg_users'])
                for x in history_std_date:
                    if x not in date_history:
                        date_history.insert(history_std_date.index(x),x)
                        value_history.insert(history_std_date.index(x),0)
                for y in current_std_date:
                    if y not in date_current:
                        date_current.insert(current_std_date.index(y),y)
                        value_current.insert(current_std_date.index(y),0)
                history_df = pd.DataFrame(zip(date_history,value_history),columns=['date','reg_users'])
                current_df = pd.DataFrame(zip(date_current,value_current),columns=['date','reg_users'])
                history_df.set_index(['date'],inplace=True)
                current_df.set_index(['date'],inplace=True)
                # print (i)
                # print (history_df)
                # print (current_df)
                if cnt == 1:
                    combined_history_df = history_df
                    combined_current_df = current_df
                else:
                    combined_history_df += history_df
                    combined_current_df += current_df
            bs_sql_history = "select substr(reg_time,1,10) as date,count(uid) as reg_users from bs_customer where reg_time between '2019-01-01 00:00:00' and '%s' group by date order by date "%(self.get_time()[0])
            bs_sql_current = "select substr(reg_time,1,10) as date,count(uid) as reg_users from bs_customer where reg_time between '%s' and '%s' group by date order by date"%(self.get_time()[0],self.get_time()[1])
            bs_history_df = execute_sql(bs_sql_history)
            bs_current_df = execute_sql(bs_sql_current)
            bs_date_history = list(bs_history_df['date'])
            bs_value_history = list(bs_history_df['reg_users'])
            bs_date_current = list(bs_current_df['date'])
            bs_value_current = list(bs_current_df['reg_users'])
            for x in history_std_date:
                if x not in bs_date_history:
                    bs_date_history.insert(history_std_date.index(x),x)
                    bs_value_history.insert(history_std_date.index(x),0)
            for y in current_std_date:
                if y not in bs_date_current:
                    bs_date_current.insert(current_std_date.index(y),y)
                    bs_value_current.insert(current_std_date.index(y),0)
            bs_history_df = pd.DataFrame(zip(bs_date_history,bs_value_history),columns = ['date','reg_users'])
            bs_current_df = pd.DataFrame(zip(bs_date_current,bs_value_current),columns = ['date','reg_users'])
            bs_history_df.set_index(['date'],inplace=True)
            bs_current_df.set_index(['date'],inplace=True)

            # print (bs_history_df)
            # print (bs_current_df)
            combined_history_df = combined_history_df+bs_history_df
            combined_current_df = combined_current_df + bs_current_df
            register_alert = combined_history_df['reg_users'].mean() - combined_history_df['reg_users'].std()  
            register_avg =   combined_history_df['reg_users'].mean()     
            sign_up_date = list(combined_current_df.index)
            sign_up_user = list(combined_current_df['reg_users'])

        elif self.prefix == 'bs':
            sql_history = "select substr(reg_time,1,10) as date,count(uid) as reg_users from bs_customer where reg_time between '2019-01-01 00:00:00' and '%s' group by date order by date "%(self.get_time()[0])
            df= execute_sql(sql_history)
            reg_history = list(df['reg_users'])
            date_history = list(df['date'])
            for i in history_std_date:
                if i not in date_history:
                    reg_history.insert(history_std_date.index(i),0)
            register_alert =(np.array(reg_history)).mean() - (np.array(reg_history)).std()
            register_avg = (np.array(reg_history)).mean() 
            sql = "select substr(reg_time,1,10) as date,count(uid) as reg_users from bs_customer where reg_time between '%s' and '%s' group by date order by date"%(self.get_time()[0],self.get_time()[1])
            register_df = execute_sql(sql)
            sign_up_date = list(register_df['date'])
            sign_up_user = list(register_df['reg_users']) 
            for i in current_std_date:
                if i not in sign_up_date:
                    sign_up_date.insert(current_std_date.index(i),i)
                    sign_up_user.insert(current_std_date.index(i),0)
        else:
            sql_history = "select substr(from_unixtime(reg_time),1,10) as date,count(uid) as reg_users from %s_customer where reg_time between 1546272000 and %d group by date order by date "%(self.prefix,self.get_time()[2])
            df= execute_sql(sql_history)
            reg_history = list(df['reg_users'])
            date_history = list(df['date'])
            for i in history_std_date:
                if i not in date_history:
                    reg_history.insert(history_std_date.index(i),0)
            register_alert =(np.array(reg_history)).mean() - (np.array(reg_history)).std()
            register_avg = (np.array(reg_history)).mean() 
            sql = "select substr(from_unixtime(reg_time),1,10) as date,count(uid) as reg_users from %s_customer where reg_time between %d and %d group by date order by date"%(self.prefix,self.get_time()[2],self.get_time()[3])
            register_df = execute_sql(sql)
            sign_up_date = list(register_df['date'])
            sign_up_user = list(register_df['reg_users'])
            for i in current_std_date:
                if i not in sign_up_date:
                    sign_up_date.insert(current_std_date.index(i),i)
                    sign_up_user.insert(current_std_date.index(i),0)

        # print ('register_alert = '),print (register_alert)
        # print ('sign_up_date= '),print (sign_up_date)
        # print ('sign_up_user = '),print (sign_up_user)
        return register_avg,register_alert,sign_up_date,sign_up_user
    
    #获取当月每天总订单及预警值
    def get_current_orders(self):
        history_std_date,current_std_date= self.get_standard_date()
        if self.prefix == 'allsite':
            cnt = 0
            for i in ['es','gg','gj','mj','sd']:
                cnt += 1
                sql_history = "select substr(from_unixtime(add_time),1,10) as date,count(orderid)  as orders from %s_order where add_time between 1546272000 and %d group by date order by date "%(i,self.get_time()[2])
                sql_current = "select substr(from_unixtime(add_time),1,10) as date,count(orderid)  as orders from %s_order where add_time between %d and %d group by date order by date "%(i,self.get_time()[2],self.get_time()[3])
                df_history = execute_sql(sql_history)
                df_current = execute_sql(sql_current)
                date_history = list(df_history['date'])
                value_history = list(df_history['orders'])
                date_current = list(df_current['date'])
                value_current = list(df_current['orders'])
                for x in history_std_date:
                    if x not in date_history:
                        date_history.insert(history_std_date.index(x),x)
                        value_history.insert(history_std_date.index(x),0)
                for y in current_std_date:
                    if y not in date_current:
                        date_current.insert(current_std_date.index(y),y)
                        value_current.insert(current_std_date.index(y),0)
                history_df = pd.DataFrame(zip(date_history,value_history),columns=['date','orders'])
                current_df = pd.DataFrame(zip(date_current,value_current),columns=['date','orders'])
                history_df.set_index(['date'],inplace=True)
                current_df.set_index(['date'],inplace=True)
                # print (i)
                # print (history_df)
                # print (current_df)
                if cnt == 1:
                    combined_history_df = history_df
                    combined_current_df = current_df
                else:
                    combined_history_df += history_df
                    combined_current_df += current_df
            
        
            bs_sql_history = "select substr(add_time,1,10) as date,count(orderid)  as orders from bs_order where add_time between '%s' and '%s' group by date order by date "%('2019-01-01 00:00:00',self.get_time()[0])
            bs_sql_current = "select substr(add_time,1,10) as date,count(orderid)  as orders from bs_order where add_time between '%s' and '%s' group by date order by date"%(self.get_time()[0],self.get_time()[1])
            bs_history_df = execute_sql(bs_sql_history)
            bs_current_df = execute_sql(bs_sql_current)
            bs_date_history = list(bs_history_df['date'])
            bs_value_history = list(bs_history_df['orders'])
            bs_date_current = list(bs_current_df['date'])
            bs_value_current = list(bs_current_df['orders'])
            for x in history_std_date:
                if x not in bs_date_history:
                    bs_date_history.insert(history_std_date.index(x),x)
                    bs_value_history.insert(history_std_date.index(x),0)
            for y in current_std_date:
                if y not in bs_date_current:
                    bs_date_current.insert(current_std_date.index(y),y)
                    bs_value_current.insert(current_std_date.index(y),0) 
            bs_history_df = pd.DataFrame(zip(bs_date_history,bs_value_history),columns = ['date','orders'])
            bs_current_df = pd.DataFrame(zip(bs_date_current,bs_value_current),columns = ['date','orders'])
            bs_history_df.set_index(['date'],inplace=True)
            bs_current_df.set_index(['date'],inplace=True)
            
            combined_history_df = combined_history_df+bs_history_df
            combined_current_df = combined_current_df + bs_current_df
            order_alert = combined_history_df['orders'].mean() - combined_history_df['orders'].std()    
            order_avg =     combined_history_df['orders'].mean()
            current_month = list(combined_current_df.index)
            current_month_order = list(combined_current_df['orders'])
        elif self.prefix == 'bs':
            sql_history= "select substr(add_time,1,10) as date,count(orderid)  as orders from bs_order where add_time between '%s' and '%s' group by date order by date"%('2019-01-01 00:00:00',self.get_time()[0])
            df_history = execute_sql(sql_history)
            order_history = list(df_history['orders'])
            date_history = list(df_history['date'])
            for i in history_std_date:
                if i not in date_history:
                    order_history.insert(history_std_date.index(i),0)
            
            order_alert = (np.array(order_history)).mean() - (np.array(order_history)).std()
            order_avg = (np.array(order_history)).mean()
            sql = "select substr(add_time,1,10) as date,count(orderid)  as orders from bs_order where add_time between '%s' and '%s' group by date order by date"%(self.get_time()[0],self.get_time()[1])
            df = execute_sql(sql)
            current_month_order = list(df['orders'])
            current_month = list(df['date'])
            for n in current_std_date:
                if n not in current_month:
                    current_month.insert(current_std_date.index(n),n)
                    current_month_order.insert(current_std_date.index(n),0)

        else:
            sql_history= "select substr(from_unixtime(add_time),1,10) as date,count(orderid)  as orders from %s_order where add_time between 1546272000 and %d group by date order by date "%(self.prefix,self.get_time()[2])
            df_history = execute_sql(sql_history)
            order_history = list(df_history['orders'])
            date_history = list(df_history['date'])
            for i in history_std_date:
                if i not in date_history:
                    order_history.insert(history_std_date.index(i),0)
            
            order_alert = (np.array(order_history)).mean() - (np.array(order_history)).std()
            order_avg = (np.array(order_history)).mean()
            sql = "select substr(from_unixtime(add_time),1,10) as date,count(orderid)  as orders from %s_order where add_time between %d and %d group by date order by date"%(self.prefix,self.get_time()[2],self.get_time()[3])
            df = execute_sql(sql)
            current_month_order = list(df['orders'])
            current_month = list(df['date'])
            for n in current_std_date:
                if n not in current_month:
                    current_month.insert(current_std_date.index(n),n)
                    current_month_order.insert(current_std_date.index(n),0)
        
        # print ('order_alert = '),print (order_alert)
        # print ('current_month= '),print (current_month)
        # print ('current_month_order = '),print (current_month_order)
        # print('总订单')
        # print (len(current_month_order))
        # print(current_month_order)
        return order_avg,order_alert,current_month_order,current_month

    #获取当月每天支付成功订单及预警值
    def get_current_paidorders(self):
        history_std_date,current_std_date= self.get_standard_date()
        if self.prefix == 'allsite':
            cnt = 0
            for i in ['es','gg','gj','mj','sd']:
                cnt += 1
                sql_history = "select substr(from_unixtime(add_time),1,10) as date,count(orderid)  as orders from %s_order where add_time between 1546272000 and %d  and flid in (2,3,4,5,9,10) group by date order by date"%(i,self.get_time()[2])
                sql_current = "select substr(from_unixtime(add_time),1,10) as date,count(orderid)  as orders from %s_order where add_time between %d and %d and flid in (2,3,4,5,9,10) group by date order by date"%(i,self.get_time()[2],self.get_time()[3])
                df_history = execute_sql(sql_history)
                df_current = execute_sql(sql_current)
                date_history = list(df_history['date'])
                value_history = list(df_history['orders'])
                date_current = list(df_current['date'])
                value_current = list(df_current['orders'])
                for x in history_std_date:
                    if x not in date_history:
                        date_history.insert(history_std_date.index(x),x)
                        value_history.insert(history_std_date.index(x),0)
                for y in current_std_date:
                    if y not in date_current:
                        date_current.insert(current_std_date.index(y),y)
                        value_current.insert(current_std_date.index(y),0)
                history_df = pd.DataFrame(zip(date_history,value_history),columns=['date','orders'])
                current_df = pd.DataFrame(zip(date_current,value_current),columns=['date','orders'])
                history_df.set_index(['date'],inplace=True)
                current_df.set_index(['date'],inplace=True)
                if cnt == 1:
                    combined_history_df = history_df
                    combined_current_df = current_df
                else:
                    combined_history_df += history_df
                    combined_current_df += current_df
            bs_sql_history ="select substr(add_time,1,10) as date,count(orderid)  as orders from bs_order where add_time between '%s' and '%s' and flid in (2,3,4,5,23) group by date order by date"%('2019-01-01 00:00:00',self.get_time()[0])
            bs_sql_current = "select substr(add_time,1,10) as date,count(orderid)  as orders from bs_order where add_time between '%s' and '%s'  and flid in (2,3,4,5,23) group by date order by date"%(self.get_time()[0],self.get_time()[1])
            bs_history_df = execute_sql(bs_sql_history)
            bs_current_df = execute_sql(bs_sql_current)
            bs_date_history = list(bs_history_df['date'])
            bs_value_history = list(bs_history_df['orders'])
            bs_date_current = list(bs_current_df['date'])
            bs_value_current = list(bs_current_df['orders'])
            for x in history_std_date:
                if x not in bs_date_history:
                    bs_date_history.insert(history_std_date.index(x),x)
                    bs_value_history.insert(history_std_date.index(x),0)
            for y in current_std_date:
                if y not in bs_date_current:
                    bs_date_current.insert(current_std_date.index(y),y)
                    bs_value_current.insert(current_std_date.index(y),0)
            bs_history_df = pd.DataFrame(zip(bs_date_history,bs_value_history),columns = ['date','orders'])
            bs_current_df = pd.DataFrame(zip(bs_date_current,bs_value_current),columns = ['date','orders'])
            bs_history_df.set_index(['date'],inplace=True)
            bs_current_df.set_index(['date'],inplace=True)
            combined_history_df = combined_history_df+bs_history_df
            combined_current_df = combined_current_df + bs_current_df
            paidorder_alert = combined_history_df['orders'].mean() - combined_history_df['orders'].std()
            paidorder_avg =     combined_history_df['orders'].mean()    
            current_month_paid = list(combined_current_df.index)
            current_month_paidorder = list(combined_current_df['orders'])
        elif self.prefix == 'bs':
            sql_history= "select substr(add_time,1,10) as date,count(orderid)  as orders from bs_order where add_time between '%s' and '%s' and flid in (2,3,4,5,23) group by date order by date"%('2019-01-01 00:00:00',self.get_time()[0])
            df_history = execute_sql(sql_history)
            paidorder_history = list(df_history['orders'])
            paidorder_history_date = list(df_history['date'])
            for i in history_std_date:
                if i not in paidorder_history_date:
                    paidorder_history.insert(history_std_date.index(i),0)
            paidorder_alert = (np.array(paidorder_history)).mean() - (np.array(paidorder_history)).std()
            paidorder_avg =(np.array(paidorder_history)).mean() 
            sql = "select substr(add_time,1,10) as date,count(orderid)  as orders from bs_order where add_time between '%s' and '%s'  and flid in (2,3,4,5,23) group by date order by date"%(self.get_time()[0],self.get_time()[1])
            df = execute_sql(sql)
            current_month_paidorder = list(df['orders'])
            current_month_paid = list(df['date'])
            for n in current_std_date:
                if n not in current_month_paid:
                    current_month_paid.insert(current_std_date.index(n),n)
                    current_month_paidorder.insert(current_std_date.index(n),0)

        else:
            sql_history= "select substr(from_unixtime(add_time),1,10) as date,count(orderid)  as orders from %s_order where add_time between 1546272000 and %d  and flid in (2,3,4,5,9,10) group by date order by date"%(self.prefix,self.get_time()[2])
            df_history = execute_sql(sql_history)
            paidorder_history = list(df_history['orders'])
            paidorder_history_date = list(df_history['date'])
            for i in history_std_date:
                if i not in paidorder_history_date:
                    paidorder_history.insert(history_std_date.index(i),0)
            paidorder_alert = (np.array(paidorder_history)).mean() - (np.array(paidorder_history)).std()
            paidorder_avg =(np.array(paidorder_history)).mean() 
            sql = "select substr(from_unixtime(add_time),1,10) as date,count(orderid)  as orders from %s_order where add_time between %d and %d and flid in (2,3,4,5,9,10) group by date order by date"%(self.prefix,self.get_time()[2],self.get_time()[3])
            df = execute_sql(sql)
            current_month_paidorder = list(df['orders'])
            current_month_paid = list(df['date'])
            for n in current_std_date:
                if n not in current_month_paid:
                    current_month_paid.insert(current_std_date.index(n),n)
                    current_month_paidorder.insert(current_std_date.index(n),0)
        
        # print ('paidorder_alert = '),print (paidorder_alert)
        # print ('current_month_paid= '),print (current_month_paid)
        # print ('current_month_paidorder = '),print (current_month_paidorder)
        # print('这是支付成功订单')
        # print (len(current_month_paidorder))
        # print (current_month_paidorder)
        return paidorder_avg,paidorder_alert,current_month_paidorder ,current_month_paid

    # 获取当月每天支付成功率
    def get_pay_rate(self):
        paidorder_avg,paid_alert,paidorder,paid_month = self.get_current_paidorders()
        order_avg,order_alert,order,current_month = self.get_current_orders()
        rate = np.array(paidorder)/np.array(order)
        rate[np.isnan(rate)==True]=0 #Nan化0
        current_payrate = list(map(lambda x:round(x,3), rate))
        

        return paid_month,current_payrate
    
    #获取当月每天营业额和预警值
    def get_current_sales(self):
        history_std_date,current_std_date= self.get_standard_date()
        if self.prefix == 'allsite':
            cnt = 0
            for i in ['es','gg','gj','mj','sd']:
                cnt += 1
                sql_history = "select substr(from_unixtime(add_time),1,10) as date,sum(money) as sales from %s_order where add_time between 1546272000 and %d and flid in (2,3,4,5,9,10) group by date order by date"%(i,self.get_time()[2])
                sql_current = "select substr(from_unixtime(add_time),1,10) as date,sum(money)  as sales from %s_order where add_time between %d and %d and flid in (2,3,4,5,23) group by date order by date"%(i,self.get_time()[2],self.get_time()[3])
                df_history = execute_sql(sql_history)
                df_current = execute_sql(sql_current)
                date_history = list(df_history['date'])
                value_history = list(df_history['sales'])
                date_current = list(df_current['date'])
                value_current = list(df_current['sales'])
                for x in history_std_date:
                    if x not in date_history:
                        date_history.insert(history_std_date.index(x),x)
                        value_history.insert(history_std_date.index(x),0)
                for y in current_std_date:
                    if y not in date_current:
                        date_current.insert(current_std_date.index(y),y)
                        value_current.insert(current_std_date.index(y),0)
                history_df = pd.DataFrame(zip(date_history,value_history),columns=['date','sales'])
                current_df = pd.DataFrame(zip(date_current,value_current),columns=['date','sales'])
                history_df.set_index(['date'],inplace=True)
                current_df.set_index(['date'],inplace=True)
                if cnt == 1:
                    combined_history_df = history_df
                    combined_current_df = current_df
                else:
                    combined_history_df += history_df
                    combined_current_df += current_df
            bs_sql_history ="select substr(add_time,1,10) as date,sum(paid_money) as sales from bs_order where add_time between '%s' and '%s' and flid in (2,3,4,5,23) group by date order by date"%('2019-01-01 00:00:00',self.get_time()[0])
            bs_sql_current ="select substr(add_time,1,10) as date,sum(paid_money)  as sales from bs_order where add_time between '%s' and '%s' and flid in (2,3,4,5,23) group by date order by date"%(self.get_time()[0],self.get_time()[1])
            bs_history_df = execute_sql(bs_sql_history)
            bs_current_df = execute_sql(bs_sql_current)
            bs_date_history = list(bs_history_df['date'])
            bs_value_history = list(bs_history_df['sales'])
            bs_date_current = list(bs_current_df['date'])
            bs_value_current = list(bs_current_df['sales'])
            for x in history_std_date:
                if x not in bs_date_history:
                    bs_date_history.insert(history_std_date.index(x),x)
                    bs_value_history.insert(history_std_date.index(x),0)
            for y in current_std_date:
                if y not in bs_date_current:
                    bs_date_current.insert(current_std_date.index(y),y)
                    bs_value_current.insert(current_std_date.index(y),0)
            bs_history_df = pd.DataFrame(zip(bs_date_history,bs_value_history),columns = ['date','sales'])
            bs_current_df = pd.DataFrame(zip(bs_date_current,bs_value_current),columns = ['date','sales'])
            bs_history_df.set_index(['date'],inplace=True)
            bs_current_df.set_index(['date'],inplace=True)
            combined_history_df = combined_history_df+bs_history_df
            combined_current_df = combined_current_df + bs_current_df
            sales_alert = combined_history_df['sales'].mean() - combined_history_df['sales'].std()
            sales_avg =  combined_history_df['sales'].mean()       
            current_sales_date = list(combined_current_df.index)
            current_month_sales =list(map(lambda x:round(x,1),list(combined_current_df['sales'])))

        elif self.prefix == 'bs':
            sql_history= "select substr(add_time,1,10) as date,sum(paid_money) as sales from bs_order where add_time between '%s' and '%s' and flid in (2,3,4,5,23) group by date order by date"%('2019-01-01 00:00:00',self.get_time()[0])
            df_history = execute_sql(sql_history)
            sales_history = list(df_history['sales'])
            sales_history_date = list(df_history['date'])
            for i in history_std_date:
                if i not in sales_history_date:
                    sales_history.insert(history_std_date.index(i),0)
            sales_alert = (np.array(sales_history)).mean() - (np.array(sales_history)).std()
            sales_avg =(np.array(sales_history)).mean() 
            
            sql = "select substr(add_time,1,10) as date,sum(paid_money)  as sales from bs_order where add_time between '%s' and '%s' and flid in (2,3,4,5,23) group by date order by date"%(self.get_time()[0],self.get_time()[1])
            df = execute_sql(sql)
            current_month_sales =list(map(lambda x:round(x,1),list(df['sales'])))
            current_sales_date = list(df['date'])
            for n in current_std_date:
                if n not in current_sales_date:
                    current_sales_date.insert(current_std_date.index(n),n)
                    current_month_sales.insert(current_std_date.index(n),0)
            
        else:
            sql_history= "select substr(from_unixtime(add_time),1,10) as date,sum(money) as sales from %s_order where add_time between 1546272000 and %d and flid in (2,3,4,5,9,10) group by date order by date"%(self.prefix,self.get_time()[2])
            df_history = execute_sql(sql_history)
            sales_history = list(df_history['sales'])
            sales_history_date = list(df_history['date'])
            for i in history_std_date:
                if i not in sales_history_date:
                    sales_history.insert(history_std_date.index(i),0)
            sales_alert = (np.array(sales_history)).mean() - (np.array(sales_history)).std()
            sales_avg =(np.array(sales_history)).mean() 
            sql = "select substr(from_unixtime(add_time),1,10) as date,sum(money)  as sales from %s_order where add_time between %d and %d and flid in (2,3,4,5,23) group by date order by date"%(self.prefix,self.get_time()[2],self.get_time()[3])
            df = execute_sql(sql)
            current_month_sales =list(map(lambda x:round(x,1),list(df['sales'])))
            current_sales_date = list(df['date'])
            for n in current_std_date:
                if n not in current_sales_date:
                    current_sales_date.insert(current_std_date.index(n),n)
                    current_month_sales.insert(current_std_date.index(n),0)
        
    
        return sales_avg,sales_alert,current_month_sales,current_sales_date 
    
    #获取各支付方式每天的支付成功率
    def get_payment_rate(self):
        history_std_date,current_std_date= self.get_standard_date()
        if self.prefix == 'allsite':
            date = None
            payment = None
            all_payment0= None
            all_payment1= None
            all_payment2= None
            all_payment3= None
            all_payment4= None
            paid_payment0= None
            paid_payment1= None
            paid_payment2= None
            paid_payment3= None
            paid_payment4= None
            
            
        elif self.prefix == 'bs':
    
            order_sql = '''SELECT  SUBSTR(add_time,1,10) as date,count(CASE WHEN payment LIKE '%%Master%%' THEN orderid END) AS 'Master',
            count(CASE WHEN payment LIKE '%%VISA%%' THEN orderid END) AS 'VISA',
            COUNT(CASE WHEN payment LIKE 'Transfer%%' then orderid END) AS 'Transe',
            COUNT(CASE WHEN payment LIKE 'PABACOIN%%' then orderid END) AS 'PABA',
            COUNT(CASE WHEN payment LIKE 'West%%' then orderid END) AS 'Western Union' 
            FROM bs_order WHERE add_time between '%s' and '%s' GROUP BY date order by date'''%(self.get_time()[0],self.get_time()[1])
            order_df = execute_sql(order_sql)
            order_df.set_index(['date'],inplace=True)
            for i in current_std_date:
                if i not in list(order_df.index):
                    order_df.loc[i] = [0,0,0,0,0]
            order_df.sort_index(inplace=True)
            date = list(order_df.index)
            payment = list(order_df.columns)
            all_payment0 = list(order_df[payment[0]])
            all_payment1 = list(order_df[payment[1]])
            all_payment2 = list(order_df[payment[2]])
            all_payment3 = list(order_df[payment[3]])
            all_payment4 = list(order_df[payment[4]])
            

            
            
            paidorder_sql = '''SELECT  SUBSTR(add_time,1,10) as date,count(CASE WHEN payment LIKE '%%Master%%' THEN orderid END) AS 'Master',
            count(CASE WHEN payment LIKE '%%VISA%%' THEN orderid END) AS 'VISA',
            COUNT(CASE WHEN payment LIKE 'Transfer%%' then orderid END) AS 'Transe',
            COUNT(CASE WHEN payment LIKE 'PABACOIN%%' then orderid END) AS 'PABA',
            COUNT(CASE WHEN payment LIKE 'West%%' then orderid END) AS 'Western Union' 
            FROM bs_order WHERE add_time between '%s' and '%s' and flid in (2,3,4,5,23) GROUP BY date  order by date'''%(self.get_time()[0],self.get_time()[1])
            paidorder_df = execute_sql(paidorder_sql)
            paidorder_df.set_index(['date'],inplace=True)
            for i in current_std_date:
                if i not in list(paidorder_df.index):
                    paidorder_df.loc[i] = [0,0,0,0,0]
            paidorder_df.sort_index(inplace=True)
            paid_payment0 = list(paidorder_df[payment[0]])
            paid_payment1 = list(paidorder_df[payment[1]])
            paid_payment2 = list(paidorder_df[payment[2]])
            paid_payment3 = list(paidorder_df[payment[3]])
            paid_payment4 = list(paidorder_df[payment[4]])
            
            
        
        else:
            order_sql = '''SELECT SUBSTR(FROM_UNIXTIME(add_time),1,10) as date,
            count(CASE WHEN payment like '360%%' THEN orderid END) AS '360',
            COUNT(CASE WHEN payment LIKE 'West%%' then orderid END) AS 'Western Union',
            COUNT(case when payment LIKE 'Transfer%%' then orderid END) as 'TransferWise',
            COUNT(case when payment like 'win pay%%' then orderid END) AS 'winpay',
            count(case when payment like 'Bringall%%' then orderid END) AS 'Bringall' FROM %s_order
            WHERE add_time between %d and %d GROUP BY date ORDER BY date  '''%(self.prefix,self.get_time()[2],self.get_time()[3])
            order_df = execute_sql(order_sql)
            order_df.set_index(['date'],inplace=True)
            for i in current_std_date:
                if i not in list(order_df.index):
                    order_df.loc[i] = [0,0,0,0,0]
            order_df.sort_index(inplace=True)
            date = list(order_df.index)
            payment = list(order_df.columns)
            all_payment0 = list(order_df[payment[0]])
            all_payment1 = list(order_df[payment[1]])
            all_payment2 = list(order_df[payment[2]])
            all_payment3 = list(order_df[payment[3]])
            all_payment4 = list(order_df[payment[4]])

            paidorder_sql = '''SELECT SUBSTR(FROM_UNIXTIME(add_time),1,10) as date,
            count(CASE WHEN payment like '360%%' THEN orderid END) AS '360',
            COUNT(CASE WHEN payment LIKE 'West%%' then orderid END) AS 'Western Union',
            COUNT(case when payment LIKE 'Transfer%%' then orderid END) as 'TransferWise',
            COUNT(case when payment like 'win pay%%' then orderid END) AS 'winpay',
            count(case when payment like 'Bringall%%' then orderid END) AS 'Bringall' FROM %s_order
            WHERE add_time between %d and %d  and flid in (2,3,4,5,9,10) GROUP BY date ORDER BY date '''%(self.prefix,self.get_time()[2],self.get_time()[3])
            paidorder_df = execute_sql(paidorder_sql)
            paidorder_df.set_index(['date'],inplace=True)
            for i in current_std_date:
                if i not in list(paidorder_df.index):
                    paidorder_df.loc[i] = [0,0,0,0,0]
            paidorder_df.sort_index(inplace=True)
            paid_payment0 = list(paidorder_df[payment[0]])
            paid_payment1 = list(paidorder_df[payment[1]])
            paid_payment2 = list(paidorder_df[payment[2]])
            paid_payment3 = list(paidorder_df[payment[3]])
            paid_payment4 = list(paidorder_df[payment[4]])
        
        return date,payment,all_payment0,all_payment1,all_payment2,all_payment3,all_payment4,paid_payment0,paid_payment1,paid_payment2,paid_payment3,paid_payment4
    
    #获取各支付方式的使用比例
    def get_payment_percentage(self):
        if self.prefix == 'allsite':
            columns = None
            order_data = None
            paidorder_data = None

        elif self.prefix == 'bs':
            order_sql = '''SELECT  count(CASE WHEN payment LIKE '%%Master%%' THEN orderid END) AS 'Master',
            count(CASE WHEN payment LIKE '%%VISA%%' THEN orderid END) AS 'VISA',
            COUNT(CASE WHEN payment LIKE 'Transfer%%' then orderid END) AS 'TransferWise',
            COUNT(CASE WHEN payment LIKE 'PABACOIN%%' then orderid END) AS 'PABA',
            COUNT(CASE WHEN payment LIKE 'West%%' then orderid END) AS 'Western Union' 
            FROM bs_order WHERE add_time between '%s' and '%s' '''%(self.get_time()[0],self.get_time()[1])
            order_df = execute_sql(order_sql)
            order_data = list(order_df.loc[0])
            columns = list(order_df.columns)

            paidorder_sql = '''SELECT  count(CASE WHEN payment LIKE '%%Master%%' THEN orderid END) AS 'Master',
            count(CASE WHEN payment LIKE '%%VISA%%' THEN orderid END) AS 'VISA',
            COUNT(CASE WHEN payment LIKE 'Transfer%%' then orderid END) AS 'TransferWise',
            COUNT(CASE WHEN payment LIKE 'PABACOIN%%' then orderid END) AS 'PABA',
            COUNT(CASE WHEN payment LIKE 'West%%' then orderid END) AS 'Western Union' 
            FROM bs_order WHERE add_time between '%s' and '%s' and flid in (2,3,4,5,23)'''%(self.get_time()[0],self.get_time()[1])
            paidorder_df = execute_sql(paidorder_sql)
            paidorder_data = list(paidorder_df.loc[0])
            
        else:
            order_sql = '''SELECT 
            count(CASE WHEN payment like '360%%' THEN orderid END) AS '360',
            COUNT(CASE WHEN payment LIKE 'West%%' then orderid END) AS 'Western Union',
            COUNT(case when payment LIKE 'Transfer%%' then orderid END) as 'TransferWise',
            COUNT(case when payment like 'win pay%%' then orderid END) AS 'winpay',
            count(case when payment like 'Bringall%%' then orderid END) AS 'Bringall' FROM %s_order
            WHERE add_time between %d and %d  '''%(self.prefix,self.get_time()[2],self.get_time()[3])
            order_df = execute_sql(order_sql)
            order_data = list(order_df.loc[0])
            columns = list(order_df.columns)
            
            paidorder_sql = '''SELECT 
            count(CASE WHEN payment like '360%%' THEN orderid END) AS '360',
            COUNT(CASE WHEN payment LIKE 'West%%' then orderid END) AS 'Western Union',
            COUNT(case when payment LIKE 'Transfer%%' then orderid END) as 'TransferWise',
            COUNT(case when payment like 'win pay%%' then orderid END) AS 'winpay',
            count(case when payment like 'Bringall%%' then orderid END) AS 'Bringall' FROM %s_order
            WHERE add_time between %d and %d  and flid in (2,3,4,5,9,10) '''%(self.prefix,self.get_time()[2],self.get_time()[3])
            paidorder_df = execute_sql(paidorder_sql)
            paidorder_data = list(paidorder_df.loc[0])
        
        return columns,order_data,paidorder_data

    #获取畅销商品排行
    def get_rank(self):
        if self.prefix == 'allsite':
            sql = '''select sku,sum(num) as amount from stock_log where add_time between '%s' and '%s' and info_stock='sub' 
                    group by sku order by amount DESC  '''%(self.get_time()[0],self.get_time()[1])
            df = execute_sql(sql)
            res_df = df.loc[0:19]
            product = list(res_df['sku'])
            amount = list(res_df['amount'])
        else:
            site_dict = {'es':'elmontyouthsoccer.com',
                        'gg':'gogoalshop.com',
                        'gj':'goaljerseys.com',
                        'mj':'minejerseys.com',
                        'sd':'soccerdealshop',
                        'bs':'bestsoccerstore'
            }
            sql = '''select sku,sum(num) as amount from stock_log where add_time between '%s' and '%s' and info_stock='sub' and site = '%s'
                    group by sku order by amount DESC '''%(self.get_time()[0],self.get_time()[1],site_dict[self.prefix])
            df = execute_sql(sql)
            res_df = df.loc[0:19]
            product = list(res_df['sku'])
            amount = list(res_df['amount'])
        
        return product,amount




