
**一、概述**

------------

   功能：MySQL向Kafka数据同步工具。基于PYTHON3.6语言开发，支持MySQL5.6,MySQL5.7,TDSQL-MySQL-C 表实时同步至Kafka中，通过解析binlog实现。

   1.1 安装python3.6

     yum -y install python36

   1.2 安装依赖

     pip3 install -r requirements.txt
   

**二、配置文件**

------------
 
   2.1 sync_mysql2es.json 参数说明
   
       "SYNC_SETTINGS"  : {
        "isSync"   : true,
        "isInit"   : true,
        "logfile"  : "sync_mysql2kafka.log",
        "batch"    : 5,
        "debug"    : "Y",
        "db"       : "test",
        "table"    : "xs1,xs2"
     },

       "MYSQL_SETTINGS" : {
          "host"  : "192.168.1.1",
          "port"  : 3306,
          "user"  : "root",
          "passwd": "root"
       },
    
       "KAFKA_SETTINGS":  {
            "host"  : "10.2.39.81",
            "port"  : "9092",
            "topic" : "test",
            "templete": {
                "data":[],
                "database":"",
                "isDdl":false,
                "old":null,
                "pkNames":[
                    "_id"
                ],
                "sql":"",
                "table":"",
                "ts":0,
                "type":""
            }
       }

        
------------

  2.2 SYNC_SETTINGS参数说明

|  参数名	 |参数描述   |
| :------------ | :------------ |
| isSync | 是否启用同步  |
| isInit | 是否全量同步  |
| logfile | 同步日志文件名  |
| batch   |全量同步批大小   |
| debug  |打开调试模式 Y,关闭：N  |
| db       | MySQL 同步库名  |
| table    | MySQL 同步表名  |


 2.3 MYSQL_SETTINGS 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host  | 配置 MySQL 数据库IP    |
| port  | 配置 MySQL 数据库PORT  |
| user | 配置 MySQL 用户   |
| passwd  | 配置 MySQL 密码  |

 2.4 KAFKA_SETTINGS 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host  | 配置 Kafka IP或域名    |
| port  | 配置 Kafka 端口  |
| topic | 配置 Kafka topic名称   |
| templete  | 配置 kafka 消息模板 |
|
------------

**三、启动同步**

   linux:
   
       nohup python3 python3 sync_mysql2kafka.py --conf=sync_mysql2kafka.json &
   
   windows:
   
       python.exe sync_mysql2kafka.py -conf=sync_mysql2kafka.json