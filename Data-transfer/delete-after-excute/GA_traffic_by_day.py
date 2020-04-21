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
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = os.path.abspath(instance.get_key_file_path())
view_id_Dict = {'sd': '167419096','gg': '183917273','es': '170634318','mj': '183931009','gj': '92659632','bs': '176331331'}
credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
metric_list = ['ga:users','ga:newUsers','ga:sessions']

now = time.localtime(time.time())
monlist = instance.get_monlist()
end_date = monlist[now.tm_mon-2][1]

#请求数据
#遍历每个网站的数据视图：
for key,viewid in view_id_Dict.items():
    count = 0
    #遍历每个数据视图的指标：
    for metric in metric_list:
        count+=1
        request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
    body={
        'reportRequests': [
        {
        'viewId': viewid,
        'dateRanges': [{'startDate': '2019-01-01', 'endDate': end_date}],
        'metrics': [{'expression': metric,'formattingType': 'INTEGER'}],
        'dimensions': [{'name': 'ga:date'}],
        'hideTotals':True,
        'hideValueRanges':True
        }]
        }
        ).execute()
    #解析数据
        date_list = []
        value_list = []
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
                    date_list.append(dimension)
                            
                for i, values in enumerate(dateRangeValues):

                    for metricHeader, value in zip(metricHeaders, values.get('values')):
                        value_list.append(int(value))
                        
        #将日期列表和指标值列表组成字典，再转化成DataFrame，方便导入数据库
        value_dict = {'date':date_list,'%s'%metric.split(':')[1]:value_list}
        df = pd.DataFrame(value_dict)
        df.fillna(0,inplace=True)
        df.set_index('date')
        #对于每个数据视图的指标，从第二个指标开始，合并至前指标的DataFrame
        if count==1:
            all_df = df
        if count==2:
            all_df['newUsers'] = df['newUsers'] 
        if count==3: 
            all_df['sessions']= df['sessions']

#添加bsco数据
        if key == 'bs':
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
    body={
        'reportRequests': [
        {
        'viewId': '201216304',
        'dateRanges': [{'startDate': '2019-09-01', 'endDate': end_date}],
        'metrics': [{'expression': metric,'formattingType': 'INTEGER'}],
        'dimensions': [{'name': 'ga:date'}],
        'hideTotals':True,
        'hideValueRanges':True
        }]
        }
        ).execute()
    #解析数据
            bsco_date_list = []
            bsco_value_list = []
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
                        bsco_date_list.append(dimension)
                                
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            bsco_value_list.append(int(value))

            zerolistlen = len(all_df) - len(bsco_value_list)
            zerolist = []
            for i in range(zerolistlen):
                zerolist.append(0)
            zero_ser = pd.Series(zerolist+bsco_value_list)

            
            if count==1:
                all_df['users'] = all_df['users'] + zero_ser

            if count==2:
                all_df['newUsers'] = all_df['newUsers'] + zero_ser

            if count==3: 
                all_df['sessions']= all_df['sessions'] + zero_ser

#添加sdco数据
        if key == 'sd':
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
    body={
        'reportRequests': [
        {
        'viewId': '200361602',
        'dateRanges': [{'startDate': '2019-09-01', 'endDate': end_date}],
        'metrics': [{'expression': metric,'formattingType': 'INTEGER'}],
        'dimensions': [{'name': 'ga:date'}],
        'hideTotals':True,
        'hideValueRanges':True
        }]
        }
        ).execute()
    #解析数据
            sdco_date_list = []
            sdco_value_list = []
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
                        sdco_date_list.append(dimension)
                                
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            sdco_value_list.append(int(value))
            
            zerolistlen = len(all_df) - len(sdco_value_list)
            zerolist = []
            for i in range(zerolistlen):
                zerolist.append(0)
            zero_ser = pd.Series(zerolist+sdco_value_list)
            
            #把sdco视图的数据加到sd上去
            if count==1:
                all_df['users'] = all_df['users'] + zero_ser

            if count==2:
                all_df['newUsers'] = all_df['newUsers'] + zero_ser

                
            if count==3: 
                all_df['sessions']= all_df['sessions'] + zero_ser


#添加gjcn数据
        if key == 'gj':
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
    body={
        'reportRequests': [
        {
        'viewId': '200806686',
        'dateRanges': [{'startDate': '2019-09-01', 'endDate': end_date}],
        'metrics': [{'expression': metric,'formattingType': 'INTEGER'}],
        'dimensions': [{'name': 'ga:date'}],
        'hideTotals':True,
        'hideValueRanges':True
        }]
        }
        ).execute()
    #解析数据
            gjco_date_list = []
            gjco_value_list = []
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
                        gjco_date_list.append(dimension)
                                
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            gjco_value_list.append(int(value))
            #两个Series相加的条件是长度相等，如果不等，就给短的补充0
            zerolistlen = len(all_df) - len(gjco_value_list)
            zerolist = []
            for i in range(zerolistlen):
                zerolist.append(0)
            zero_ser = pd.Series(zerolist+gjco_value_list)
            
            #把gjcn视图的数据加到gj视图上去
            if count==1:
                all_df['users'] = all_df['users'] + zero_ser

            if count==2:
                all_df['newUsers'] = all_df['newUsers'] + zero_ser
                               
            if count==3: 
                all_df['sessions']= all_df['sessions'] + zero_ser
                
            
        

    engine = create_engine('mysql+pymysql://'+instance.get_account()+':'+instance.get_password()+'@localhost:3306/'+instance.get_db_name())
    all_df.to_sql(key+'_'+'traffic_byday',con = engine,if_exists='replace',index = False)
    print key+'data write into mysql success'
