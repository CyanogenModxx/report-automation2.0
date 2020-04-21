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
#获取昨天的日期，格式："2019-01-01"
today = datetime.date.today()
if  today.day !=1:
    yestoday = (today-datetime.timedelta(1)).__format__('%Y-%m-%d')
else :
    yestoday = (datetime.date(today.year,today.month,1)-datetime.timedelta(1)).__format__('%Y-%m-%d')
    

#请求数据
#遍历每个网站的数据视图：

for key,viewid in view_id_Dict.items():
    
    value_list = []
    dimension_list = []
    date_list = []
    term_list = []
    request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
body={
    'reportRequests': [
    {
    'viewId': viewid,
    'dateRanges': [{'startDate': yestoday, 'endDate': yestoday}],
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
    all_df = df
    
#添加bs.co数据       
    if key == 'bs':
        bsco_date_list = []
        bsco_term_list = []
        bsco_calendar = []
        bsco_dimension_list = []
        bsco_value_list = []
        request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
    body={
        'reportRequests': [
        {
        'viewId': '201216304',
        'dateRanges': [{'startDate': yestoday, 'endDate': yestoday}],
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
        bsco_df = pd.DataFrame(zip(bsco_date_list,bsco_term_list,bsco_value_list),columns=['date','term','search_times'])
        all_df = all_df.append(bsco_df)

#添加sd.co数据       
    if key == 'sd':
        sdco_date_list = []
        sdco_term_list = []
        sdco_calendar = []
        sdco_dimension_list = []
        sdco_value_list = []
        request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
    body={
        'reportRequests': [
        {
        'viewId': '200361602',
        'dateRanges': [{'startDate': yestoday, 'endDate': yestoday}],
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
        sdco_df = pd.DataFrame(zip(sdco_date_list,sdco_term_list,sdco_value_list),columns=['date','term','search_times'])
        all_df = all_df.append(sdco_df)
  

#添加gj.cn数据       
    if key == 'gj':
        cnt3 = 0
        gjco_date_list = []
        gjco_term_list = []
        gjco_calendar = []
        gjco_dimension_list = []
        gjco_value_list = []
        request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
    body={
        'reportRequests': [
        {
        'viewId': '200806686',
        'dateRanges': [{'startDate': yestoday, 'endDate': yestoday}],
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
        gjco_df = pd.DataFrame(zip(gjco_date_list,gjco_term_list,gjco_value_list),columns=['date','term','search_times'])
        all_df = all_df.append(gjco_df)
    
    
    engine = create_engine('mysql+pymysql://'+instance.get_account()+':'+instance.get_password()+'@localhost:3306/'+instance.get_db_name())
    all_df.to_sql(key+'_'+'search_term_by_day',con=engine,if_exists='append',index=False)
    print key+' data write into mysql success'