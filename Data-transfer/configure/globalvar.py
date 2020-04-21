# -*- coding:utf-8 -*-
#该文件与instance.py文件用于作为以GA开头文件的全局变量文件。该文件定义全局变量，instance.py是实例。


class GlobalVar:
    
    def __init__(self,account,password,db_name,key_file_path,monlist):
        self.account = account
        self.password = password
        self.db_name = db_name
        self.key_file_path = key_file_path
        self.monlist = monlist

    def get_account(self):
        return self.account
    def get_password(self):
        return self.password
    def get_db_name(self):
        return self.db_name
    def get_key_file_path(self):
        return self.key_file_path
    def get_monlist(self):
        return self.monlist


    