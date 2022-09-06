
**一、概述**

------------


   功能：doris向mysql离线数据同步工具。基于PYTHON3.6语言开发，支持doris表同步至MySQL5.6,MySQL5.7,TDSQL-MySQL-C ，基于时间戳实现。

   1.1 安装python3.6

     yum -y install python36

   1.2 安装依赖

     pip3 install -r requirements.txt
   

**二、配置文件**

------------
 
   2.1 sync_mysql2es.json 参数说明
   
    "SYNC_SETTINGS"  : {
        "logfile"  : "sync_mysql2es.log",
        "batch_size"    : 5,
        "incr_batch_size" : 2,
        "sync_time_type" : "day",        
        "debug"    : "Y"
    },
    
    "DORIS_SETTINGS" : {
        "host"  : "192.168.3.5",
        "port"  : 3306,
        "user"  : "root",
        "passwd": "root"
        "schema" : "block_app",
        "table"  : "app_recommend:update_time:700"
    },
    
    "MYSQL_SETTINGS" : {
        "host"  : "192.168.3.5",
        "port"  : 3306,
        "user"  : "root",
        "passwd": "root"
    },
    
------------

  2.2 SYNC_SETTINGS参数说明

|  参数名	 |参数描述   |
| :------------ | :------------ |
| logfile | 同步日志文件名  |
| batch_size   |全量批大小   |
| incr_batch_size | 增量批大小  |
| sync_time_type | 同步时间类型(day,hour,min) |
| debug  |打开调试模式 Y,关闭：N  |


 2.3 DORIS_SETTINGS 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host  | 配置 doris 数据库IP    |
| port  | 配置 doris 数据库PORT  |
| user | 配置 doris 用户   |
| passwd  | 配置 doris 密码  |
| schema | 配置 doris 连接数据库   |
| table  | 配置 MySQL 表，格式:table1:incr_column1:incr_time1,支持配置多张表，以逗号分隔|

 2.4 MYSQL_SETTINGS 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host  | 配置 MYSQL 数据库IP或域名    |
| port  | 配置 MYSQL 端口  |
| user  | 配置 MYSQL 连接用户  |
| passwd | 配置 MYSQL 连接密码   |
| schema  | 配置 MYSQL 索引名  |

------------

**三、启动同步**

   linux:
   
       nohup python3 python3 sync_doris2mysql.py --conf=sync_doris2mysql.json &
   
   windows:
   
       python.exe sync_doris2mysql.py -conf=sync_doris2mysql.json

