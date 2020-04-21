# Report Automation 2.0

## Flask

相关代码在 report-automation2.0 目录下 

部署环境

    python3.7.4
    flask 1.0.2
    mysql 5.6.40
    uwsgi 2.0.18

#### 部署方案

1. 安装python模块

pip3 install -r requirements.txt
    
2. 修改配置config.py

修改为生产数据库
    
3. 配置uwsgi

修改uwsgi.ini文件

    [uwsgi]
    socket = 127.0.0.1:8008
    chdir = /home/report-automation/report-automation2.0/
    wsgi-file = /home/report-automation/report-automation2.0/report.py
    callable = app
    processes = 2
    threads = 2
    daemonize = /home/report-automation/server.log
    touch-reload = /home/report-automation/report-automation2.0/
    stats = 127.0.0.1:9191
    #stats=/home/report-automation/uwsgi.status
    pidfile=/home/report-automation/uwsgi.pid

4. 启动uwsgi

/usr/local/python37/bin/uwsgi --ini /home/report-automation/uwsgi.ini

5. 配置nginx

编辑nginx的配置文件

    server {
        listen 52151;
        server_name report.taotens.com;
    
        access_log /home/wwwlogs/report_automation.log;
    
        location / {
            include uwsgi_params;
            uwsgi_pass 127.0.0.1:8008;
        }
    
        location /stats {
            include uwsgi_params;
            uwsgi_pass 127.0.0.1:9191;
        }
    }
    
6. 创建用户

暂无图形化创建用户入口，需要在服务器上创建

    # python3
    >>> from app.models import User
    >>> from app import db
    >>> u = User(username='username')
    >>> u.set_password('password')
    >>> db.session.add(u)
    >>> db.session.commit()

## GA数据导入

相关代码在 Data-transfer 目录下

1. 安装python2.7模块

```
google-api-python-client
oauth2client
sqlalchemy
numpy (version 1.16.4)
pandas (version 0.24.2)
```

2. 修改配置

congfigure 文件夹为数据库地址等信息配置文件

3. 运行数据脚本

GA开头的.py文件，需要使用国外网络环境，且使用python27执行
其他.py文件，使用python3.x执行

- delete-after-excute 主要导入历史数据，执行一次即可
- dec 主要导入历史数据，执行一次即可
- excute-byday 每天执行一次，可以设置为每天06：00执行。他们的作用是将前一天的数据导入到数据库。
- excute-bymonth 每月1号06：00执行一次。他们的作用是将前一个月的数据导入到数据库。
