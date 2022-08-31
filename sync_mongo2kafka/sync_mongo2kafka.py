#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/12/14 11:02
# @Author : 马飞
# @File : sync_mongo_oplog.py
# @Software: PyCharm

import time
import pymongo
import json
import datetime
import sys,os
import logging
import argparse
from uuid import UUID
from bson.objectid import ObjectId
from pymongo.cursor import CursorType
from kafka  import KafkaProducer
from kafka.errors import KafkaError

'''
  1.初始全量同步：表加锁+全量初始化+释放锁+增量同步
  2.增量同步成功后增量检查点至文件中
  3.每次启动都会进行全量同步，数据会重复
  4.根据袁林提供的模板写消息DML
  5.增加开关是否写DDL事件
  6.KAFKA报错:Failed to update metadata after 60.0 secs.
  7.全量同步开始记录ts,全量同步后增量同步从大于等于ts开始同步
  
'''

KAFKA_INSERT_TEMPLETE = {
    "data":[],
    "database":"",
    "isDdl":False,
    "old":None,
    "pkNames":[
        "_id"
    ],
    "sql":"",
    "table":"",
    "ts":0,
    "type":"INSERT"
}

KAFKA_DELETE_TEMPLETE = {
    "data":[],
    "database":"",
    "isDdl":False,
    "old":None,
    "pkNames":[
        "_id"
    ],
    "sql":"",
    "table":"",
    "ts":0,
    "type":"DELETE"
}

KAFKA_UPDATE_TEMPLETE = {
    "data":[],
    "database":"",
    "isDdl":False,
    "old":None,
    "pkNames":[
        "_id"
    ],
    "sql":"",
    "table":"",
    "ts":0,
    "type":"UPDATE"
}


'''
    功能：读json配置文件转为dict
'''
def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg


'''
    功能：将datatime类型序列化json可识别类型
'''
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        elif isinstance(obj, UUID):
            return obj.hex
        elif isinstance(obj, ObjectId):
            return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)

'''
    功能：mongo 单实例认证
    入口：mongo 连接串，格式：IP:PORT:DB:USER:PASSWD
    出口：mongo 数据库连接对象
'''
def get_ds_mongo_auth(mongodb_str):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    conn          = pymongo.MongoClient('mongodb://{0}:{1}/'.format(ip,int(port)))
    db            = conn[service]
    if user != '' and password != '':
        db.authenticate(user, password)
        db = conn['local']
    return db

'''
    功能：mongo 单实例认证,连接指定DB
    入口：mongo 连接串，格式：IP:PORT:DB:USER:PASSWD
    出口：mongo 数据库连接对象
'''
def get_ds_mongo_auth_db(mongodb_str,conn_db):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    conn          = pymongo.MongoClient('mongodb://{0}:{1}/'.format(ip,int(port)))
    db            = conn[service]
    if user != '' and password != '':
        db.authenticate(user, password)
        db = conn[conn_db]
    return db


'''
    功能：mongo 副本集认证
    入口：mongo 连接串，格式：IP1,IP2,IP3:PORT1,PORT2,PORT3:DB:USER:PASSWD
    出口：mongo 数据库连接对象
'''
def get_ds_mongo_auth_replset(mongodb_str,replset_name):
    ip            = mongodb_str.split(':')[0]
    port          = mongodb_str.split(':')[1]
    service       = mongodb_str.split(':')[2]
    user          = mongodb_str.split(':')[3]
    password      = mongodb_str.split(':')[4]
    replstr       = ''

    for v in range(len(ip.split(','))):
        replstr=replstr+'{0}:{1},'.format(ip.split(',')[v],port.split(',')[v])
    conn  = pymongo.MongoClient('mongodb://{0},replicaSet={1}'.format(replstr[0:-1],replset_name))
    db    = conn[service]

    if user!='' and password !='' :
       db.authenticate(user, password)
       db = conn['local']
    return db

'''
    功能：打印配置
'''
def print_cfg(config):
    print('Mongodb config:')
    print_dict(config['mongo'])
    print('Kafka config:')
    print_dict(config['kafka'])

'''
    功能：kafka 生产对象类   
'''
class Kafka_producer():
    '''
      使用kafka的生产模块
    '''
    def __init__(self, kafkahost, kafkaport, kafkatopic):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.kafkaServer = [(k+':'+str(v)) for k,v in dict(zip(self.kafkaHost,self.kafkaPort)).items()]
        self.producer = KafkaProducer(bootstrap_servers=self.kafkaServer)

    def sendjsondata(self, params):
        try:
            parmas_message = json.dumps(params, cls=DateEncoder)
            producer = self.producer
            producer.send(self.kafkatopic, parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(str(e))

'''
    功能：格式化输出字典对象   
'''
def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

'''
    功能：datatime对象增加8小时   
'''
def preprocessor(result):
    for key in result['o']:
        if is_valid_datetime(result['o'][key]):
            result['o'][key] = result['o'][key] + datetime.timedelta(hours=8)
    return result

'''
    功能：判断对象是否为datetime类型  
'''
def is_valid_datetime(strdate):
    if isinstance(strdate,datetime.datetime):
        return True
    else:
        return False


'''
    功能：写日志 
'''
def write_log(doc):
    doc['ts'] = str(doc['ts'])
    if doc['op'] =='i' or doc['op'] =='d' :
       doc['o']['_id'] = str(doc['o']['_id'])

    if doc['op']=='u':
       doc['o']['_id'] = str(doc['o']['_id'])
       doc['o2']['_id'] = str(doc['o2']['_id'])

    doc=preprocessor(doc)
    logging.info(json.dumps(doc, cls=DateEncoder,ensure_ascii=False, indent=4, separators=(',', ':'))+'\n')

'''
    功能：获取mongo,es连接对象
'''
def get_config(cfg):
    config={}
    producer = Kafka_producer(cfg['KAFKA_SETTINGS']['host'].split(','), cfg['KAFKA_SETTINGS']['port'].split(','), cfg['KAFKA_SETTINGS']['topic'])
    config['producer'] = producer
    config['kafka'] = cfg['KAFKA_SETTINGS']
    config['sync_settings'] = cfg['SYNC_SETTINGS']

    if cfg['MONGO_SETTINGS'].get('replset') is None or cfg['MONGO_SETTINGS'].get('replset') == '':
        mongo = get_ds_mongo_auth('{0}:{1}:{2}:{3}:{4}'.
                                  format(cfg['MONGO_SETTINGS']['host'],
                                         cfg['MONGO_SETTINGS']['port'],
                                         cfg['MONGO_SETTINGS']['db'],
                                         cfg['MONGO_SETTINGS']['user'],
                                         cfg['MONGO_SETTINGS']['passwd']))
    else:
        mongo = get_ds_mongo_auth_replset('{0}:{1}:{2}:{3}:{4}'.
                                      format(cfg['MONGO_SETTINGS']['host'],
                                             cfg['MONGO_SETTINGS']['port'],
                                             cfg['MONGO_SETTINGS']['db'],
                                             cfg['MONGO_SETTINGS']['user'],
                                             cfg['MONGO_SETTINGS']['passwd']),
                                             cfg['MONGO_SETTINGS']['replset'])

    config['mongo'] = cfg['MONGO_SETTINGS']
    config['mongo_db'] = mongo
    config['sync_table'] = ['{}.{}'.format(cfg['MONGO_SETTINGS'].get('db_name'),t) for t in cfg['MONGO_SETTINGS'].get('tab_name').split(',')]
    return config


'''
    功能：同步所有库数据
'''
def full_sync(config):
    if (config['mongo']['db_name'] == '' or config['mongo']['db_name'] is None) \
            and (config['mongo']['tab_name'] == '' or config['mongo']['tab_name'] is None):
       pass

    if config['mongo']['db_name'] is not None and config['mongo']['tab_name'] is not None :
       for i in config['sync_table']:
          print('full sync :{}...'.format(i))
          db = get_ds_mongo_auth_db('{0}:{1}:{2}:{3}:{4}'.
                                 format(config['mongo']['host'],
                                        config['mongo']['port'],
                                        config['mongo']['db'],
                                        config['mongo']['user'],
                                        config['mongo']['passwd']),
                                        i.split('.')[0])
          tab = i.split('.')[1]
          cr = db[tab]
          rs = cr.find({})
          counter = 1
          batch = []
          for e in rs:
             # v_json = {}
             # v_json['op'] = 'insert'
             # v_json['obj'] = i
             # v_json['val'] = e
             # v_json['val']['_id'] = str(v_json['val']['_id'])
             e['_id'] = str(e['_id'])
             batch.append(e)
             if counter % config['sync_settings']['batch'] == 0:
                v_json = KAFKA_INSERT_TEMPLETE
                v_json['data'] = batch
                v_json['database'] = config['mongo']['db_name']
                v_json['table'] = i.split('.')[1]
                v_json['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                config['producer'].sendjsondata(v_json)
                v_json = json.dumps(v_json,
                                   cls=DateEncoder,
                                   ensure_ascii=False,
                                   indent=4,
                                   separators=(',', ':')) + '\n'
                if config['sync_settings']['debug'] == 'Y':
                   print(v_json)
                   logging.info(v_json)
                batch = []
             counter+=1

          # write last batch
          v_json = KAFKA_INSERT_TEMPLETE
          v_json['data'] = batch
          v_json['database'] = config['mongo']['db_name']
          v_json['table'] = i.split('.')[1]
          v_json['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
          config['producer'].sendjsondata(json.dumps(v_json,cls=DateEncoder))
          v_json = json.dumps(v_json,
                              cls=DateEncoder,
                              ensure_ascii=False,
                              indent=4,
                              separators=(',', ':')) + '\n'
          if config['sync_settings']['debug'] == 'Y':
              print(v_json)


def incr_sync(config):
    cr = config['mongo_db']['oplog.rs']
    first = next(cr.find().sort('$natural', pymongo.DESCENDING).limit(-1))
    ts = first['ts']
    print('insr sync:',config['sync_table'])
    cursor = cr.find({'ts': {'$gt': ts}}, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True)
    while cursor.alive:
        for doc in cursor:
            ts = doc['ts']
            db = doc['ns'].split('.')[0]
            if config['mongo']['db_name'] == db and  config['mongo']['tab_name'] is not None:
                if doc['ns'] in config['sync_table']:
                    write_log(doc)
                    if doc['op'] == 'i':
                        # v_json['op'] = 'insert'
                        # v_json['obj'] = doc['ns']
                        # v_json['val'] = doc['o']
                        # v_json['val']['_id'] = str(v_json['val']['_id'])
                        if config['sync_settings']['debug'] == 'Y':
                           print('insert doc=', doc)
                        v_json = KAFKA_INSERT_TEMPLETE
                        doc['o']['_id'] = str(doc['o']['_id'] )
                        v_json['data'] = [doc['o']]
                        v_json['database'] = doc['ns'].split('.')[0]
                        v_json['table'] = doc['ns'].split('.')[1]
                        v_json['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                        config['producer'].sendjsondata(v_json)
                        v_json = json.dumps(v_json,
                                            cls=DateEncoder,
                                            ensure_ascii=False,
                                            indent=4,
                                            separators=(',', ':')) + '\n'
                        if config['sync_settings']['debug'] == 'Y':
                            print(v_json)
                            logging.info(v_json)


                    if doc['op'] == 'd':
                        # v_json['op'] = 'delete'
                        # v_json['obj'] = doc['ns']
                        # v_json['val'] = str(doc['o']['_id'])
                        if config['sync_settings']['debug'] == 'Y':
                           print('delete doc=', doc)
                        v_json = KAFKA_DELETE_TEMPLETE
                        doc['o']['_id'] = str(doc['o']['_id'])
                        v_json['data'] = [doc['o']]
                        v_json['database'] = doc['ns'].split('.')[0]
                        v_json['table'] = doc['ns'].split('.')[1]
                        v_json['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                        config['producer'].sendjsondata(v_json)
                        v_json = json.dumps(v_json,
                                            cls=DateEncoder,
                                            ensure_ascii=False,
                                            indent=4,
                                            separators=(',', ':')) + '\n'
                        if config['sync_settings']['debug'] == 'Y':
                            print(v_json)
                            logging.info(v_json)


                    if doc['op'] == 'u':
                        # v_json['op'] = 'update'
                        # v_json['obj'] = doc['ns']
                        # if doc['o'].get('$set', None) is not None:
                        #     v_json['val'] = doc['o']['$set']
                        # else:
                        #     v_json['val'] = doc['o']
                        #     v_json['val']['_id'] = str(v_json['val']['_id'])
                        # v_json['where'] = doc['o2']
                        # v_json['where']['_id'] = str(v_json['where']['_id'])
                        if config['sync_settings']['debug'] == 'Y':
                           print('update doc=', doc)
                        v_json = KAFKA_UPDATE_TEMPLETE

                        v_json['data'] = [doc['o']['$set']] if doc['o'].get('$set', None) is not None else [doc['o']]
                        if v_json['data'][0].get('_id'):
                           v_json['data'][0]['_id'] = str(v_json['data'][0]['_id'])
                        v_json['database'] = doc['ns'].split('.')[0]
                        v_json['table'] = doc['ns'].split('.')[1]
                        v_json['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                        config['producer'].sendjsondata(v_json)
                        v_json = json.dumps(v_json,
                                            cls=DateEncoder,
                                            ensure_ascii=False,
                                            indent=4,
                                            separators=(',', ':')) + '\n'
                        if config['sync_settings']['debug'] == 'Y':
                            print(v_json)
                            logging.info(v_json)


            if (config['mongo']['db_name']  is  None or config['mongo']['db_name'] =='') \
                    and (config['mongo']['tab_name'] is None or  config['mongo']['tab_name'] =='') :
                write_log(doc)
                if doc['op'] == 'i':
                    # v_json['op'] = 'insert'
                    # v_json['obj'] = doc['ns']
                    # v_json['val'] = doc['o']
                    # v_json['val']['_id'] = str(v_json['val']['_id'])
                    # config['producer'].sendjsondata(v_json)
                    if config['sync_settings']['debug'] == 'Y':
                        print('insert doc=', doc)
                    v_json = KAFKA_INSERT_TEMPLETE
                    doc['o']['_id'] = str(doc['o']['_id'])
                    v_json['data'] = doc['o']
                    v_json['database'] = doc['ns'].split('.')[0]
                    v_json['table'] = doc['ns'].split('.')[1]
                    v_json['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                    config['producer'].sendjsondata(v_json)
                    v_json = json.dumps(v_json,
                                        cls=DateEncoder,
                                        ensure_ascii=False,
                                        indent=4,
                                        separators=(',', ':')) + '\n'
                    if config['sync_settings']['debug'] == 'Y':
                        print(v_json)
                        logging.info(v_json)


                if doc['op'] == 'd':
                    if config['sync_settings']['debug'] == 'Y':
                        print('delete doc=', doc)
                    v_json = KAFKA_DELETE_TEMPLETE
                    v_json['data'] = doc['o']
                    v_json['database'] = doc['ns'].split('.')[0]
                    v_json['table'] = doc['ns'].split('.')[1]
                    v_json['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                    config['producer'].sendjsondata(v_json)
                    v_json = json.dumps(v_json,
                                        cls=DateEncoder,
                                        ensure_ascii=False,
                                        indent=4,
                                        separators=(',', ':')) + '\n'
                    if config['sync_settings']['debug'] == 'Y':
                        print(v_json)
                        logging.info(v_json)

                if doc['op'] == 'u':
                    if config['sync_settings']['debug'] == 'Y':
                        print('update doc=', doc)
                    v_json = KAFKA_UPDATE_TEMPLETE
                    v_json['data'] = doc['o']['$set'] if doc['o'].get('$set', None) is not None else doc['o']
                    v_json['old'] = doc['o2']
                    v_json['database'] = doc['ns'].split('.')[0]
                    v_json['table'] = doc['ns'].split('.')[1]
                    v_json['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                    config['producer'].sendjsondata(v_json)
                    v_json = json.dumps(v_json,
                                        cls=DateEncoder,
                                        ensure_ascii=False,
                                        indent=4,
                                        separators=(',', ':')) + '\n'
                    if config['sync_settings']['debug'] == 'Y':
                        print(v_json)
                        logging.info(v_json)

def init_log(config):
    os.system('>{}'.format(config['mongo']['logfile']))

def parse_param():
    parser = argparse.ArgumentParser(description='Resolve mysql binlogfile.')
    parser.add_argument('--conf', help='配置文件', default=None, required=True)
    args = parser.parse_args()
    return args

'''
    功能：主函数   
'''
def main():
   # read config
   args = parse_param()
   cfg = read_json(args.conf)

   if not cfg['SYNC_SETTINGS']['isSync']:
      print('Sync is disabled!' )
      sys.exit(0)

   # init config
   config = get_config(cfg)

   # init logger
   logging.basicConfig(
       filename='{}.{}.log'.format(config['sync_settings']['logfile'], datetime.datetime.now().strftime("%Y-%m-%d")),
       format='[%(asctime)s-%(levelname)s:%(message)s]',
       level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

   # print config
   print_cfg(config)

   # full sync
   if config['sync_settings']['isInit']:
      full_sync(config)

   # incr sync
   incr_sync(config)

if __name__ == "__main__":
     main()

