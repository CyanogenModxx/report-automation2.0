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
monlist = instance.get_monlist()
now = time.localtime(time.time())
date_range = monlist[:now.tm_mon+(now.tm_year-2019)*12-1]
    
metric_list = ['ga:users','ga:newUsers','ga:sessions']
#请求数据
#遍历每个网站的数据视图：

for key,viewid in view_id_Dict.items():
    count = 0
    for metric in metric_list:
        count+=1
        value_list = []
        for r in date_range:
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': viewid,
            'dateRanges': [{'startDate': r[0], 'endDate': r[1]}],
            'metrics': [{'expression': metric,'formattingType': 'INTEGER'}],
            'hideTotals':True,
            'hideValueRanges':True
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
                                             
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            value_list.append(int(value))
        month_list = []    
        for i in date_range:
            month = i[0][:7].split('-')
            month_list.append(month[0]+month[1])                       
        if count == 1:
            value_dict = {'month':month_list,'users':value_list}
            df = pd.DataFrame(value_dict)
            all_df = df
        if count == 2:
            all_df['newUsers'] = pd.Series(value_list)
        if count == 3:
            all_df['sessions'] = pd.Series(value_list)
   
#添加bsco数据
        if key == 'bs':
            bs_date_range = monlist[8:(now.tm_year-2019)*12+now.tm_mon-1]
            for br in bs_date_range:
                request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
            body={
                'reportRequests': [
                {
                'viewId': '201216304',
                'dateRanges': [{'startDate': br[0], 'endDate': br[1]}],
                'metrics': [{'expression': metric,'formattingType': 'INTEGER'}],
                'hideTotals':True,
                'hideValueRanges':True
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
                                                
                        for i, values in enumerate(dateRangeValues):
                            for metricHeader, value in zip(metricHeaders, values.get('values')):
                                x = int(value)
                
                #到这里了，将bsco对应的指标数据加到all——df对应的位置上
                idx = br[0].split('-')[0]+br[0].split('-')[1]
                index = month_list.index(idx)

                if count == 1:
                    all_df['users'][index]= all_df['users'][index]+x
                if count == 2:
                    all_df['newUsers'][index]= all_df['newUsers'][index]+ x
                if count == 3:
                    all_df['sessions'][index]= all_df['sessions'][index]+ x

#添加sdco数据
        if key == 'sd':
            sd_date_range = monlist[8:(now.tm_year-2019)*12+now.tm_mon-1]
            for sr in sd_date_range:
                request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
            body={
                'reportRequests': [
                {
                'viewId': '200361602',
                'dateRanges': [{'startDate': sr[0], 'endDate':sr[1]}],
                'metrics': [{'expression': metric,'formattingType': 'INTEGER'}],
                'hideTotals':True,
                'hideValueRanges':True
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
                                                    
                        for i, values in enumerate(dateRangeValues):
                            for metricHeader, value in zip(metricHeaders, values.get('values')):
                                x = int(value)
                idx = sr[0].split('-')[0]+sr[0].split('-')[1]
                index = month_list.index(idx)
                if count == 1:
                    all_df['users'][index]= all_df['users'][index]+ x
                if count == 2:
                    all_df['newUsers'][index] = all_df['newUsers'][index] + x
                if count == 3:
                    all_df['sessions'][index] = all_df['sessions'][index] + x

#添加gjcn数据
        if key == 'gj':
            gj_date_range = monlist[8:(now.tm_year-2019)*12+now.tm_mon-1]
            for gr in gj_date_range:
                request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
            body={
                'reportRequests': [
                {
                'viewId': '200806686',
                'dateRanges': [{'startDate': gr[0], 'endDate': gr[1]}],
                'metrics': [{'expression': metric,'formattingType': 'INTEGER'}],
                'hideTotals':True,
                'hideValueRanges':True
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
                                                
                        for i, values in enumerate(dateRangeValues):
                            for metricHeader, value in zip(metricHeaders, values.get('values')):
                                x = int(value)
                idx = gr[0].split('-')[0]+gr[0].split('-')[1]
                index = month_list.index(idx)
                if count == 1:
                    all_df['users'][index] = all_df['users'][index]+ x
                if count == 2:
                    all_df['newUsers'][index] = all_df['newUsers'][index] + x
                if count == 3:
                    all_df['sessions'][index] = all_df['sessions'][index] + x
#添加gg.com.cn数据
        if key == 'gg':
            gg_date_range = monlist[11:(now.tm_year-2019)*12+now.tm_mon-1]
            for gdr in gg_date_range:
                request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
            body={
                'reportRequests': [
                {
                'viewId': '206476635',
                'dateRanges': [{'startDate': gdr[0], 'endDate': gdr[1]}],
                'metrics': [{'expression': metric,'formattingType': 'INTEGER'}],
                'hideTotals':True,
                'hideValueRanges':True
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
                                                
                        for i, values in enumerate(dateRangeValues):
                            for metricHeader, value in zip(metricHeaders, values.get('values')):
                                x = int(value)
                idx = gdr[0].split('-')[0]+gdr[0].split('-')[1]
                index = month_list.index(idx)
                if count == 1:
                    all_df['users'][index] = all_df['users'][index]+ x
                if count == 2:
                    all_df['newUsers'][index] = all_df['newUsers'][index] + x
                if count == 3:
                    all_df['sessions'][index] = all_df['sessions'][index] + x
    
    engine = create_engine('mysql+pymysql://'+instance.get_account()+':'+instance.get_password()+'@localhost:3306/'+instance.get_db_name())
    all_df.to_sql(key+'_'+'traffic_by_month',con=engine,if_exists='replace',index=False)
    print key+' data write into mysql success'


            


        
                    

       