import os
import pymysql
from sqlalchemy import create_engine
import pandas as pd


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://report_auto:abadnhDt2RjOGOQ6@localhost:3306/report_automation'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_POOL_RECYCLE = 499
    SQLALCHEMY_POOL_TIMEOUT= 30

# def Database():
#     # DB = create_engine('mysql+pymysql://report_auto:abadnhDt2RjOGOQ6@localhost:3306/report_automation')
#     DB = pymysql.connect('localhost','report_auto','abadnhDt2RjOGOQ6','report_automation')
#     return DB 

def execute_sql(sql):
    DB = pymysql.connect('localhost','report_auto','abadnhDt2RjOGOQ6','report_automation')
    res = pd.read_sql(sql,DB)
    DB.close()
    return res