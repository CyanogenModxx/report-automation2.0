1: delete-after-excute 文件夹中的文件，在12月2日执行一次后删除
2：excute-byday 文件夹中的文件从12月2日起，每天执行一次，可以设置为每天06：00执行。他们的作用是将前一天的数据导入到数据库。
3：excute-bymonth 文件夹中的文件在12月2日执行一次，此后每个月1号06：00执行一次。他们的作用是将前一个月的数据导入到数据库。
4: dec 文件夹是将12月1日数据导入数据库的文件，执行一次后不再使用。
4：GA开头的文件需要在墙外运行，且python 2.7环境。其余则为python3.x环境下运行。
5:congfigure 文件夹为数据库地址 等信息配置文件。它是一个python 自定义库，需要将其添加到python2.7类库中，否则Data-transfer中的某些py文件不能导入该自定义库。
  添加到类库的方法 ：

  进入 python 2.7 shell  

  import sys 
  sys.path.append('congfigure文件夹路径')

6：python2.7环境包
    google-api-python-client
    oauth2client
    sqlalchemy
    numpy (version 1.16.4)
    pandas (version 0.24.2)
