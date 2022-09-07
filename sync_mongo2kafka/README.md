

**一、概述**

------------


   功能：MongoDB向Kafka数据同步工具。基于PYTHON3.6语言开发，支持MongoDB表实时同步至kafka中，通过解析oplog实现。

   1.1 安装python3.6

     yum -y install python36

   1.2 安装依赖

     pip3 install -r requirements.txt
   

**二、配置文件**

------------
 
 2.1 sync_mongo2kakfa.json 示例
    
    # KAFKA 单机
 
         "SYNCSETTINGS":  {
            "isSync"   : true,
            "isInit"   : true,
            "logfile"  : "sync_mongo2kafka.log",
            "batch"    : 5,
            "debug"    : "Y",
            "sync_gap" : 3
         }
   
       "MONGO_SETTINGS" : {
            "host"     : "192.168.1.1",
            "port"     : "48011",
            "db"       : "admin",
            "user"     : "admin",
            "passwd"   : "admin",
            "db_name"  : "test",
            "tab_name" : "xs1,xs2"
        },

        "KAFKA_SETTINGS":  {
                "host"  : "10.2.39.81",
                "port"  : "9092",
                "topic" : "test",
                "templete" : {
                    "data":[],
                    "database":"",
                    "dataType":"",
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
   
    
     # KAFKA 集群配置
   
        "KAFKA_SETTINGS":  {
            "host"  : "10.2.39.81,10.2.39.82,10.2.39.83",
            "port"  : "9092,9092,9092",
            "topic" : "test"
        }
    }
    
  2.2 SYNC_SETTINGS 参数说明：
------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
|  |

| isSync | 是否启用同步  |
| isInit | 是否进行全量初始化  |
| logfile | 同步日志文件名  |
| batch | 实始化量同步批大小  |
| debug | 是否打印调试信息  |
| sync_gap | 全量同步批同步休息秒数，用于调试  |

    
  2.3 MONGO_SETTINGS 参数说明：
------------

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


 2.4 KAFKA_SETTINGS 参数说明：

------------

|  参数名	 |参数描述   |
| :------------ | :------------ |
| host  | 配置 kafka 数据库IP    |
| port  | 配置 kafka 数据库PORT  |
| topic | 配置 kafka topic名称   |



------------

**三、启动同步**

   linux:
   
       nohup python3 sync_mongo2kafka.py -conf=sync_mongo2kafka.json &
   
   windows:
   
       python.exe sync_mongo2kafka.py -conf=sync_mongo2kafka.json
       python.exe KafkaConsumer.py