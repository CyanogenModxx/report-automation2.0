# -*- coding:utf-8 -*-

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
import os
import pandas as pd 
import pymysql
import datetime
import time
import sys
sys.path.append('..') 
from configure import instance
#gg在8月13日才开始统计搜索词
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = os.path.abspath(instance.get_key_file_path())
view_id_Dict = {'sd': '167419096','es': '170634318','mj': '183931009','gj': '92659632','bs': '176331331'}
credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
monlist = instance.get_monlist()
#自动捕捉当前日期，确定查询的日期范围 
now = time.localtime(time.time())
end_day = monlist[now.tm_mon-2][1]
#生成一个日期列表
calendar_list = []
begin_date = datetime.datetime.strptime('2019-05-10','%Y-%m-%d')
end_date =  datetime.datetime.strptime(end_day ,'%Y-%m-%d')
while begin_date<=end_date:
    date_str = begin_date.strftime('%Y-%m-%d')
    calendar_list.append(date_str)
    begin_date += datetime.timedelta(days=1)


#请求数据
#遍历每个网站的数据视图：

for key,viewid in view_id_Dict.items():
    count = 0
    for r in calendar_list:
        count += 1
        value_list = []
        dimension_list = []
        date_list = []
        term_list = []
        request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
    body={
        'reportRequests': [
        {
        'viewId': viewid,
        'dateRanges': [{'startDate': r, 'endDate': r}],
        'metrics': [{'expression': 'ga:searchUniques','formattingType': 'INTEGER'}],
        'hideTotals':True,
        'hideValueRanges':True,
        'dimensions':[{'name':'ga:date'},{'name':'ga:searchKeyword'}]
        }]
        }
        ).execute()
    #解析数据
        for report in request.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])

                for header, dimension in zip(dimensionHeaders, dimensions):
                    header = header.encode('utf-8')
                    dimension = dimension.encode('utf-8')
                    dimension_list.append(dimension)
                                       
                for i, values in enumerate(dateRangeValues):
                    for metricHeader, value in zip(metricHeaders, values.get('values')):
                        value_list.append(int(value))

        for x in range(0,len(dimension_list)):
            if x%2 == 0:
                date_list.append(dimension_list[x])
            else:
                term_list.append(dimension_list[x])
        df = pd.DataFrame(zip(date_list,term_list,value_list),columns = ['date','term','search_times'])
        if count == 1:
            all_df = df
        else:
            all_df = all_df.append(df)
#添加bs.co数据       
    if key == 'bs':
        bsco_date_list = []
        bsco_term_list = []
        bsco_calendar = []
        bsco_dimension_list = []
        bsco_value_list = []
        cnt1 = 0
        start_time = datetime.datetime.strptime('2019-09-01','%Y-%m-%d')
        end_time =  datetime.datetime.strptime(end_day ,'%Y-%m-%d')
        while start_time<=end_time: 
            date_str = start_time.strftime('%Y-%m-%d')
            bsco_calendar.append(date_str)
            start_time += datetime.timedelta(days=1)

        for date in bsco_calendar:
            cnt1 += 1
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': '201216304',
            'dateRanges': [{'startDate': date, 'endDate': date}],
            'metrics': [{'expression': 'ga:searchUniques','formattingType': 'INTEGER'}],
            'hideTotals':True,
            'hideValueRanges':True,
            'dimensions':[{'name':'ga:date'},{'name':'ga:searchKeyword'}]
            }]
            }
            ).execute()
        #解析数据
            for report in request.get('reports', []):
                columnHeader = report.get('columnHeader', {})
                dimensionHeaders = columnHeader.get('dimensions', [])
                metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

                for row in report.get('data', {}).get('rows', []):
                    dimensions = row.get('dimensions', [])
                    dateRangeValues = row.get('metrics', [])

                    for header, dimension in zip(dimensionHeaders, dimensions):
                        header = header.encode('utf-8')
                        dimension = dimension.encode('utf-8')
                        bsco_dimension_list.append(dimension)
                        
                        
                    for i, values in enumerate(dateRangeValues):

                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            bsco_value_list.append(int(value))
            for x in range(len(bsco_dimension_list)):
                if x%2 == 0:
                    bsco_date_list.append(bsco_dimension_list[x])
                else:
                    bsco_term_list.append(bsco_dimension_list[x])
            bsco_singleday_df = pd.DataFrame(zip(bsco_date_list,bsco_term_list,bsco_value_list),columns=['date','term','search_times'])
            if cnt1 == 1:
                bsco_df = bsco_singleday_df
            else:
                bsco_df = bsco_df.append(bsco_singleday_df)
        all_df = all_df.append(bsco_df)

#添加sd.co数据       
    if key == 'sd':
        cnt2 = 0
        sdco_date_list = []
        sdco_term_list = []
        sdco_calendar = []
        sdco_dimension_list = []
        sdco_value_list = []
        start_time = datetime.datetime.strptime('2019-09-01','%Y-%m-%d')
        end_time =  datetime.datetime.strptime(end_day ,'%Y-%m-%d')
        while start_time<=end_time:
            date_str = start_time.strftime('%Y-%m-%d')
            sdco_calendar.append(date_str)
            start_time += datetime.timedelta(days=1)

        for date in sdco_calendar:
            cnt2 += 1
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': '200361602',
            'dateRanges': [{'startDate': date, 'endDate': date}],
            'metrics': [{'expression': 'ga:searchUniques','formattingType': 'INTEGER'}],
            'hideTotals':True,
            'hideValueRanges':True,
            'dimensions':[{'name':'ga:date'},{'name':'ga:searchKeyword'}]
            }]
            }
            ).execute()
        #解析数据
            for report in request.get('reports', []):
                columnHeader = report.get('columnHeader', {})
                dimensionHeaders = columnHeader.get('dimensions', [])
                metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

                for row in report.get('data', {}).get('rows', []):
                    dimensions = row.get('dimensions', [])
                    dateRangeValues = row.get('metrics', [])

                    for header, dimension in zip(dimensionHeaders, dimensions):
                        header = header.encode('utf-8')
                        dimension = dimension.encode('utf-8')
                        sdco_dimension_list.append(dimension)
                                                
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            sdco_value_list.append(int(value))
            for x in range(len(sdco_dimension_list)):
                if x%2 == 0:
                    sdco_date_list.append(sdco_dimension_list[x])
                else:
                    sdco_term_list.append(sdco_dimension_list[x])
            sdco_singleday_df = pd.DataFrame(zip(sdco_date_list,sdco_term_list,sdco_value_list),columns=['date','term','search_times'])
            if cnt2 == 1:
                sdco_df = sdco_singleday_df
            else:
                sdco_df = sdco_df.append(sdco_singleday_df)
        all_df = all_df.append(sdco_df)
  

#添加gj.cn数据       
    if key == 'gj':
        cnt3 = 0
        gjco_date_list = []
        gjco_term_list = []
        gjco_calendar = []
        gjco_dimension_list = []
        gjco_value_list = []
        start_time = datetime.datetime.strptime('2019-09-01','%Y-%m-%d')
        end_time =  datetime.datetime.strptime(end_day ,'%Y-%m-%d')
        while start_time<=end_time:
            date_str = start_time.strftime('%Y-%m-%d')
            gjco_calendar.append(date_str)
            start_time += datetime.timedelta(days=1)

        for date in gjco_calendar:
            cnt3 += 1
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': '200806686',
            'dateRanges': [{'startDate': date, 'endDate': date}],
            'metrics': [{'expression': 'ga:searchUniques','formattingType': 'INTEGER'}],
            'hideTotals':True,
            'hideValueRanges':True,
            'dimensions':[{'name':'ga:date'},{'name':'ga:searchKeyword'}]
            }]
            }
            ).execute()
        #解析数据
            for report in request.get('reports', []):
                columnHeader = report.get('columnHeader', {})
                dimensionHeaders = columnHeader.get('dimensions', [])
                metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

                for row in report.get('data', {}).get('rows', []):
                    dimensions = row.get('dimensions', [])
                    dateRangeValues = row.get('metrics', [])

                    for header, dimension in zip(dimensionHeaders, dimensions):
                        header = header.encode('utf-8')
                        dimension = dimension.encode('utf-8')
                        gjco_dimension_list.append(dimension)
                        
                        
                    for i, values in enumerate(dateRangeValues):

                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            gjco_value_list.append(int(value))
            for x in range(len(gjco_dimension_list)):
                if x%2 == 0:
                    gjco_date_list.append(gjco_dimension_list[x])
                else:
                    gjco_term_list.append(gjco_dimension_list[x])
            gjco_singleday_df = pd.DataFrame(zip(gjco_date_list,gjco_term_list,gjco_value_list),columns=['date','term','search_times'])
            if cnt3 == 1:
                gjco_df = gjco_singleday_df
            else:
                gjco_df = gjco_df.append(gjco_singleday_df)
        all_df = all_df.append(gjco_df)
    
    engine = create_engine('mysql+pymysql://'+instance.get_account()+':'+instance.get_password()+'@localhost:3306/'+instance.get_db_name())
    all_df.to_sql(key+'_'+'search_term_by_day',con=engine,if_exists='replace',index=False)
    print key+' data write into mysql success'