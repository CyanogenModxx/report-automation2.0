# -*- coding:utf-8 -*-
import sys,os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(BASE_DIR))
import globalvar

Var = globalvar.GlobalVar(
        account = 'report_auto',
        password = 'abadnhDt2RjOGOQ6',
        db_name = 'report_automation',
        key_file_path = os.path.join(BASE_DIR,"client_secrets.json"),
        monlist = [['2019-01-01','2019-01-31'],['2019-02-01','2019-02-28'],['2019-03-01','2019-03-31'],['2019-04-01','2019-04-30'],['2019-05-01','2019-05-31'],
['2019-06-01','2019-06-30'],['2019-07-01','2019-07-31'],['2019-08-01','2019-08-31'],['2019-09-01','2019-09-30'],['2019-10-01','2019-10-31']
,['2019-11-01','2019-11-30'],['2019-12-01','2019-12-31'],['2020-01-01','2020-01-31'],['2020-02-01','2020-02-29'],['2020-03-01','2020-03-31'],
['2020-04-01','2020-04-30'],['2020-05-01','2020-05-31'],['2020-06-01','2020-06-30']]

)

def get_account():
    return Var.get_account()
def get_password():
    return Var.get_password()
def get_db_name():
    return Var.get_db_name()
def get_key_file_path():
    return Var.get_key_file_path()
def get_monlist():
    return Var.get_monlist()
