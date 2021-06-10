1、功能描述： 
     
   基于PYTHON3开发，Oracle向Mysql数据同步工具，支持表全量、增量方式同步至MySQL数据库中，基于时间戳实现。

2、安装依赖

   yum install python3
   pip3 install -r requirement.txt  -i https://pypi.douban.com/simple

3、配置文件

   sync_oracle2mysql.json
   
   #Oracle 配置：
        "oracle_server": {
             "host": "192.168.20.128",
             "port": "1521",
             "instance":"orcl",
             "user": "scott",
             "password":"tiger"
        }
   
   #MySQL配置：
      "mysql_server": {
         "host": "10.2.39.17",
         "port": "23306",
         "user": "puppet",
         "password":"Puppet@123",
         "database":"test",
         "charset":"utf8"
        }
   
    sync_tables：同步表列表,emp:hirdate:5（表示同步emp表，按hirdate列进行增量同步最近5天数据)
    batch_size : 同步批大小（每次同步多少行)
    
    说明：配置文件需要和程序放在相同目录下
    
3、启动同步

   python3 sync_oracle2mysql.py
