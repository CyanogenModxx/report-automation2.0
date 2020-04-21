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
#请求数据
#遍历每个网站的数据视图：

for key,viewid in view_id_Dict.items():
    dimension_list = []
    value_list = []
    count = 0
    for r in date_range:
        count += 1
        request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
    body={
        'reportRequests': [
        {
        'viewId': viewid,
        'dateRanges': [{'startDate': r[0], 'endDate': r[1]}],
        'metrics': [{'expression':'ga:users','formattingType': 'INTEGER'}],
        'dimensions':[{'name':'ga:channelGrouping'}],
        'hideTotals':True,
        'hideValueRanges':True
        }]
        }
        ).execute()
    #解析数据
        dimension_list = []
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
                    dimension_list.append(dimension)
                    
                for i, values in enumerate(dateRangeValues):
                    for metricHeader, value in zip(metricHeaders, values.get('values')):
                        value = int(value)
                        value_list.append(value)
        
        x = dimension_list.index('(Other)')
        dimension_list[x] = 'Promote'

        #不同的网站其各个来源可能缺失（也就是0，但不会被显示），为方便后续的数据处理，生成一个标准的数据来源，也就是给缺失的来源赋值0。
        standard_dimension = {'Direct','Organic Search','Affiliates','Referral','Social','Promote','Email'}
        for n in standard_dimension:
            if n not in dimension_list:
                dimension_list.append(n)
                value_list.append(0)

        source_df = pd.DataFrame(zip(dimension_list,value_list),columns = ['group','users'] )
        source_df.set_index(['group'],inplace=True)

        #把Referral,Social,Affiliates这三个来源合并为SocialNetwork，Promote和Email合并为Promotion
        SocialNetwork = source_df.loc['Referral','users']+source_df.loc['Social','users']+source_df.loc['Affiliates','users']
        promotion = source_df.loc['Email','users']+source_df.loc['Promote','users']
        promotion_df = pd.DataFrame([['Promotion',promotion]],columns = ['group','users'])
        seed_df = pd.DataFrame([['SocialNetwork',SocialNetwork]],columns = ['group','users'])
        seed_df.set_index('group',inplace=True)
        promotion_df.set_index('group',inplace=True)
        source_df = source_df.append(seed_df).append(promotion_df)
        source_df = source_df.loc[['Direct','Organic Search','SocialNetwork','Promotion']]
        #添加月份列
        month_list = [r[0].split('-')[0]+r[0].split('-')[1],r[0].split('-')[0]+r[0].split('-')[1],r[0].split('-')[0]+r[0].split('-')[1],r[0].split('-')[0]+r[0].split('-')[1]]
        source_df['month'] = month_list
        #把各个月份的DataFrame合并为一个DataFrame
        if count == 1:
            all_df = source_df
        else:
            all_df = pd.concat([all_df,source_df],axis=0)
    source = list(all_df.index)
    all_df.set_index('month',inplace=True)
    

    
#添加bsco数据
    if key == 'bs':
        bs_count = 0
        bs_date_range = monlist[8:(now.tm_year-2019)*12+now.tm_mon-1]
        for br in bs_date_range:
            bs_count += 1
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': '201216304',
            'dateRanges': [{'startDate': br[0], 'endDate': br[1]}],
            'metrics': [{'expression':'ga:users','formattingType': 'INTEGER'}],
            'dimensions':[{'name':'ga:channelGrouping'}],
            'hideTotals':True,
            'hideValueRanges':True
            }]
            }
            ).execute()
        #解析数据
            bsco_dimension_list = []
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
                        bsco_dimension_list.append(dimension)
                        
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            value = int(value)
                            bsco_value_list.append(value)
                                                                                      
            x = bsco_dimension_list.index('(Other)')
            bsco_dimension_list[x] = 'Promote'

            #不同的网站其各个来源可能缺失（也就是0，但不会被显示），为方便后续的数据处理，生成一个标准的数据来源，也就是给缺失的来源赋值0。
            standard_dimension = {'Direct','Organic Search','Affiliates','Referral','Social','Promote','Email'}
            for n in standard_dimension:
                if n not in bsco_dimension_list:
                    bsco_dimension_list.append(n)
                    bsco_value_list.append(0)

            bsco_df = pd.DataFrame(zip(bsco_dimension_list,bsco_value_list),columns = ['group','users'] )
            bsco_df.set_index(['group'],inplace=True)

            #把Referral,Social,Affiliates这三个来源合并为SocialNetwork，Promote和Email合并为Promotion
            SocialNetwork = bsco_df.loc['Referral','users']+bsco_df.loc['Social','users']+ bsco_df.loc['Affiliates','users']
            promotion = bsco_df.loc['Email','users']+bsco_df.loc['Promote','users']
            promotion_df = pd.DataFrame([['Promotion',promotion]],columns = ['group','users'])
            seed_df = pd.DataFrame([['SocialNetwork',SocialNetwork]],columns = ['group','users'])
            seed_df.set_index('group',inplace=True)
            promotion_df.set_index('group',inplace=True)
            bsco_df = bsco_df.append(seed_df).append(promotion_df)
            bsco_df = bsco_df.loc[['Direct','Organic Search','SocialNetwork','Promotion']]

            bsco_monlist = [br[0].split('-')[0]+br[0].split('-')[1],br[0].split('-')[0]+br[0].split('-')[1],br[0].split('-')[0]+br[0].split('-')[1],
                            br[0].split('-')[0]+br[0].split('-')[1]]
            bsco_df['month'] = bsco_monlist
            if bs_count == 1:
                bsco_alldf = bsco_df
            else:
                bsco_alldf = pd.concat([bsco_alldf,bsco_df],axis=0)
        bsco_alldf.set_index('month',inplace = True)
        bsco_alldf.astype(float)
        bsco_value = list(bsco_alldf['users'])
        for i in range(32):
            bsco_value.insert(0,0)
        bsco_alldf=pd.DataFrame(zip(list(all_df.index),bsco_value),columns=['month','users'])
        bsco_alldf.set_index('month',inplace=True)
        all_df = all_df+bsco_alldf
       


#添加sdco数据
    if key == 'sd':
        sd_count = 0
        sd_date_range = monlist[8:(now.tm_year-2019)*12+now.tm_mon-1]
        for sr in sd_date_range:
            sd_count += 1
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': '200361602',
            'dateRanges': [{'startDate': sr[0], 'endDate': sr[1]}],
            'metrics': [{'expression':'ga:users','formattingType': 'INTEGER'}],
            'dimensions':[{'name':'ga:channelGrouping'}],
            'hideTotals':True,
            'hideValueRanges':True
            }]
            }
            ).execute()
        #解析数据
            sdco_dimension_list = []
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
                        sdco_dimension_list.append(dimension)
                        
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            value = int(value)
                            sdco_value_list.append(value)
            
            x = sdco_dimension_list.index('(Other)')
            sdco_dimension_list[x] = 'Promote'

            #不同的网站其各个来源可能缺失（也就是0，但不会被显示），为方便后续的数据处理，生成一个标准的数据来源，也就是给缺失的来源赋值0。
            standard_dimension = {'Direct','Organic Search','Affiliates','Referral','Social','Promote','Email'}
            for n in standard_dimension:
                if n not in sdco_dimension_list:
                    sdco_dimension_list.append(n)
                    sdco_value_list.append(0)

            sdco_df = pd.DataFrame(zip(sdco_dimension_list,sdco_value_list),columns = ['group','users'] )
            sdco_df.set_index(['group'],inplace=True)

            #把Referral,Social,Affiliates这三个来源合并为SocialNetwork，Promote和Email合并为Promotion
            SocialNetwork = sdco_df.loc['Referral','users']+sdco_df.loc['Social','users']+ sdco_df.loc['Affiliates','users']
            promotion = sdco_df.loc['Email','users']+sdco_df.loc['Promote','users']
            promotion_df = pd.DataFrame([['Promotion',promotion]],columns = ['group','users'])
            seed_df = pd.DataFrame([['SocialNetwork',SocialNetwork]],columns = ['group','users'])
            seed_df.set_index('group',inplace=True)
            promotion_df.set_index('group',inplace=True)
            sdco_df = sdco_df.append(seed_df).append(promotion_df)
            sdco_df = sdco_df.loc[['Direct','Organic Search','SocialNetwork','Promotion']]
            sdco_monlist = [sr[0].split('-')[0]+sr[0].split('-')[1],sr[0].split('-')[0]+sr[0].split('-')[1],sr[0].split('-')[0]+sr[0].split('-')[1],
                            sr[0].split('-')[0]+sr[0].split('-')[1]]
            sdco_df['month'] = sdco_monlist
            if sd_count == 1:
                sdco_alldf = sdco_df
            else:
                sdco_alldf = pd.concat([sdco_alldf,sdco_df],axis=0)
        sdco_alldf.set_index('month',inplace = True)
        sdco_alldf.astype(float)
        sdco_value = list(sdco_alldf['users'])
        for i in range(32):
            sdco_value.insert(0,0)
        sdco_alldf=pd.DataFrame(zip(list(all_df.index),sdco_value),columns=['month','users'])
        sdco_alldf.set_index('month',inplace=True)
        all_df = all_df+sdco_alldf
        

#添加gjcn数据
    if key == 'gj':
        gj_count = 0
        gj_date_range = monlist[8:(now.tm_year-2019)*12+now.tm_mon-1]
        for gr in gj_date_range:
            gj_count += 1
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': '200806686',
            'dateRanges': [{'startDate': gr[0], 'endDate': gr[1]}],
            'metrics': [{'expression':'ga:users','formattingType': 'INTEGER'}],
            'dimensions':[{'name':'ga:channelGrouping'}],
            'hideTotals':True,
            'hideValueRanges':True
            }]
            }
            ).execute()
        #解析数据
            gjco_dimension_list = []
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
                        gjco_dimension_list.append(dimension)
                        
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            value = int(value)
                            gjco_value_list.append(value)
            
            x = gjco_dimension_list.index('(Other)')
            gjco_dimension_list[x] = 'Promote'

            #不同的网站其各个来源可能缺失（也就是0，但不会被显示），为方便后续的数据处理，生成一个标准的数据来源，也就是给缺失的来源赋值0。
            standard_dimension = {'Direct','Organic Search','Affiliates','Referral','Social','Promote','Email'}
            for n in standard_dimension:
                if n not in gjco_dimension_list:
                    gjco_dimension_list.append(n)
                    gjco_value_list.append(0)

            gjco_df = pd.DataFrame(zip(gjco_dimension_list,gjco_value_list),columns = ['group','users'] )
            gjco_df.set_index(['group'],inplace=True)

            #把Referral,Social,Affiliates这三个来源合并为SocialNetwork，Promote和Email合并为Promotion
            SocialNetwork = gjco_df.loc['Referral','users']+gjco_df.loc['Social','users']+ gjco_df.loc['Affiliates','users']
            promotion = gjco_df.loc['Email','users']+gjco_df.loc['Promote','users']
            promotion_df = pd.DataFrame([['Promotion',promotion]],columns = ['group','users'])
            seed_df = pd.DataFrame([['SocialNetwork',SocialNetwork]],columns = ['group','users'])
            seed_df.set_index('group',inplace=True)
            promotion_df.set_index('group',inplace=True)
            gjco_df = gjco_df.append(seed_df).append(promotion_df)
            gjco_df = gjco_df.loc[['Direct','Organic Search','SocialNetwork','Promotion']]
            gjco_monlist = [gr[0].split('-')[0]+gr[0].split('-')[1],gr[0].split('-')[0]+gr[0].split('-')[1],gr[0].split('-')[0]+gr[0].split('-')[1],
                            gr[0].split('-')[0]+gr[0].split('-')[1]]
            gjco_df['month'] = gjco_monlist
            if gj_count == 1:
                gjco_alldf = gjco_df
            else:
                gjco_alldf = pd.concat([gjco_alldf,gjco_df],axis=0)
        gjco_alldf.set_index('month',inplace = True)
        gjco_alldf.astype(float)
        gjco_value = list(gjco_alldf['users'])
        for i in range(32):
            gjco_value.insert(0,0)
        gjco_alldf=pd.DataFrame(zip(list(all_df.index),gjco_value),columns=['month','users'])
        gjco_alldf.set_index('month',inplace=True)
        all_df = all_df+gjco_alldf

#添加gg.com.cn数据
    if key == 'gg':
        gg_count = 0
        gg_date_range = monlist[11:(now.tm_year-2019)*12+now.tm_mon-1]
        for gdr in gg_date_range:
            gg_count += 1
            request = build('analyticsreporting', 'v4', credentials=credentials).reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': '206476635',
            'dateRanges': [{'startDate': gdr[0], 'endDate': gdr[1]}],
            'metrics': [{'expression':'ga:users','formattingType': 'INTEGER'}],
            'dimensions':[{'name':'ga:channelGrouping'}],
            'hideTotals':True,
            'hideValueRanges':True
            }]
            }
            ).execute()
        #解析数据
            ggco_dimension_list = []
            ggco_value_list = []
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
                        ggco_dimension_list.append(dimension)
                        
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            value = int(value)
                            ggco_value_list.append(value)
            
            x = ggco_dimension_list.index('(Other)')
            ggco_dimension_list[x] = 'Promote'

            #不同的网站其各个来源可能缺失（也就是0，但不会被显示），为方便后续的数据处理，生成一个标准的数据来源，也就是给缺失的来源赋值0。
            standard_dimension = {'Direct','Organic Search','Affiliates','Referral','Social','Promote','Email'}
            for n in standard_dimension:
                if n not in ggco_dimension_list:
                    ggco_dimension_list.append(n)
                    ggco_value_list.append(0)

            ggco_df = pd.DataFrame(zip(ggco_dimension_list,ggco_value_list),columns = ['group','users'] )
            ggco_df.set_index(['group'],inplace=True)

            #把Referral,Social,Affiliates这三个来源合并为SocialNetwork，Promote和Email合并为Promotion
            SocialNetwork = ggco_df.loc['Referral','users']+ggco_df.loc['Social','users']+ ggco_df.loc['Affiliates','users']
            promotion = ggco_df.loc['Email','users']+ggco_df.loc['Promote','users']
            promotion_df = pd.DataFrame([['Promotion',promotion]],columns = ['group','users'])
            seed_df = pd.DataFrame([['SocialNetwork',SocialNetwork]],columns = ['group','users'])
            seed_df.set_index('group',inplace=True)
            promotion_df.set_index('group',inplace=True)
            ggco_df = ggco_df.append(seed_df).append(promotion_df)
            ggco_df = ggco_df.loc[['Direct','Organic Search','SocialNetwork','Promotion']]
            ggco_monlist = [gdr[0].split('-')[0]+gdr[0].split('-')[1],gdr[0].split('-')[0]+gdr[0].split('-')[1],gdr[0].split('-')[0]+gdr[0].split('-')[1],
                            gdr[0].split('-')[0]+gdr[0].split('-')[1]]
            ggco_df['month'] = ggco_monlist
            if gg_count == 1:
                ggco_alldf = ggco_df
            else:
                ggco_alldf = pd.concat([ggco_alldf,ggco_df],axis=0)
        ggco_alldf.set_index('month',inplace = True)
        ggco_alldf.astype(float)
        ggco_value = list(ggco_alldf['users'])
        for i in range(44):
            ggco_value.insert(0,0)
        ggco_alldf=pd.DataFrame(zip(list(all_df.index),ggco_value),columns=['month','users'])
        ggco_alldf.set_index('month',inplace=True)
        all_df = all_df+ggco_alldf
    
    all_df['group'] = source
    all_df['month'] = all_df.index 
 
    engine = create_engine('mysql+pymysql://'+instance.get_account()+':'+instance.get_password()+'@localhost:3306/'+instance.get_db_name())
    all_df.to_sql(key+'_'+'traffic_source_by_month',con=engine,if_exists='replace',index=False)
    print key+' data write into mysql success'      

