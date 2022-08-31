
**一、概述**

------------


   功能：MySQL向ElasticSearch数据同步工具。基于PYTHON3.6语言开发，支持MySQL5.6,MySQL5.7,TDSQL-MySQL-C 表实时同步至ElasticSearch中，通过解析oplog实现。

   1.1 安装python3.6

     yum -y install python36

   1.2 安装依赖

     pip3 install -r requirements.txt
   

**二、配置文件**

------------
 
   2.1 sync_mysql2es.json 参数说明
   
    "SYNC_SETTINGS"  : {
        "isSync"   : true,
        "level"    : "1",
        "db"       : "block_cluster",
        "table"    : "cluster_info",
        "logfile"  : "sync_mysql2es.log",
        "batch"    : 5,
        "debug"    : "Y"
    },
    
    "MYSQL_SETTINGS" : {
        "host"  : "192.168.3.5",
        "port"  : 3306,
        "user"  : "root",
        "passwd": "Dev21@block2022"
    },
    
    "ES_SETTINGS": {
        "host": "192.168.3.43",
        "port": "9200",
        "schema": "http",
        "user": "elastic",
        "passwd": "21block@2022",
        "index": "21block_cluster_info2",
        "batch": 3,
        "mapping": {
            "mappings": {
            }
        }
    }

        
------------

  2.2 SYNC_SETTINGS参数说明

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host     |  MongoDB 数据库IP,复本集配置IP以逗号分隔 |
| port     | MongoDB 数据库PORT,复本集配置PORT以逗号分隔  |
| db       | MongoDB 认证数据库名  |
| user     |MongoDB 用户名  |
| passwd   |MongoDB 口令   |
| replset  |MongoDB复本集名称，为空时表示连接单实例   |
| db_name  | 定义监控的数据库名称  |
| tab_name | 定义监控的表名称,多张表用逗号隔开  |
| isSync | 是否启用同步  |
| isInit | 是否进行全量初始化  |
| logfile | 同步日志文件名  |


 2.3 KAFKA_SETTINGS 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host  | 配置 kafka 数据库IP    |
| port  | 配置 kafka 数据库PORT  |
| topic | 配置 kafka topic名称   |

 2.4 ES_SETTINGS 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host  | 配置 ES 数据库IP或域名    |
| port  | 配置 ES 端口  |
| schema | 配置 ES 协议值为http或https   |
| user  | 配置 ES 连接用户  |
| passwd | 配置 ES 连接密码   |
| index  | 配置 ES 索引名  |
| mapping | 配置 索引 mapping   |
------------

**三、启动同步**

   linux:
   
       nohup python3 python3 mysql2es_sync.py --conf=mysql2es_sync.json &
   
   windows:
   
       d:\apps\python3.6\python.exe mysql2es_sync.py -conf=mysql2es_sync.json

