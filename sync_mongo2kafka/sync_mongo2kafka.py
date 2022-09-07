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
import warnings
from uuid import UUID
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from pymongo.cursor import CursorType
from kafka  import KafkaProducer
from kafka.errors import KafkaError

'''
    功能：读json配置文件转为dict
'''
def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    cfg['cfg_file'] = file
    cfg['ckpt_file'] = file.replace('json', 'ckpt')
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

def preprocessor2(doc):
    for key in doc:
        if is_valid_datetime(doc[key]):
            doc[key] = doc[key] + datetime.timedelta(hours=8)
    return doc

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

def write_ckpt(cfg):
    ck = {
       'ts': {
         "time" : cfg['ts'].time,
         "inc" : cfg['ts'].inc
        },
       'pid':os.getpid()
    }
    with open(cfg['ckpt_file'], 'w') as f:
        f.write(json.dumps(ck, ensure_ascii=False, indent=4, separators=(',', ':')))
    print('update checkpoint:{}'.format(cfg['ts']))
    logging.info('update checkpoint:{}'.format(cfg['ts']))

def check_ckpt(cfg):
    return os.path.isfile(cfg['ckpt_file'])

def read_ckpt(cfg):
    with open(cfg['ckpt_file'], 'r') as f:
        contents = f.read()
    if  contents == '':
        return ''
    else:
        binlog = json.loads(contents)
        return binlog

def get_ts(cfg):
    cr = cfg['mongo_db']['oplog.rs']
    first = next(cr.find().sort('$natural', pymongo.DESCENDING).limit(-1))
    return {'time':first['ts'].time,'inc':first['ts'].inc}

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

'''
    功能：获取mongo,es连接对象
'''
def get_config(cfg):
    config=cfg
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

    if check_ckpt(cfg):
        ckpt = read_ckpt(cfg)
        ts = ckpt['ts']
        pid = ckpt['pid']
    else:
        ts = get_ts(config)
        pid = os.getpid()

    config['ts'] = ts
    config['pid'] = pid
    upd_ckpt(config)
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
             e['_id'] = str(e['_id'])
             batch.append(preprocessor2(e))
             if counter % config['sync_settings']['batch'] == 0:
                msg = dict(config['kafka']['templete'])
                msg['data'] = batch
                msg['database'] = config['mongo']['db_name']
                msg['dataType'] = 'FULL'
                msg['table'] = i.split('.')[1]
                msg['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                msg['type'] = 'INSERT'
                config['producer'].sendjsondata(msg)
                if config['sync_settings']['sync_gap']>0:
                   logging.info('full sync sleep {}s'.format(config['sync_settings']['sync_gap']))
                   time.sleep(config['sync_settings']['sync_gap'])

                out = json.dumps(msg,
                                 cls=DateEncoder,
                                 ensure_ascii=False,
                                 indent=4,
                                 separators=(',', ':')) + '\n'
                if config['sync_settings']['debug'] == 'Y':
                   logging.info(out)
                batch = []
             counter+=1

          # write last batch
          msg = dict(config['kafka']['templete'])
          msg['data'] = batch
          msg['database'] = config['mongo']['db_name']
          msg['dataType'] = 'FULL'
          msg['table'] = i.split('.')[1]
          msg['ts'] = int( round(time.time() * 1000))
          msg['type'] = 'INSERT'
          config['producer'].sendjsondata(msg)
          out = json.dumps(msg,
                              cls=DateEncoder,
                              ensure_ascii=False,
                              indent=4,
                              separators=(',', ':')) + '\n'
          if config['sync_settings']['debug'] == 'Y':
             logging.info(out)

def incr_sync(config):
    cr = config['mongo_db']['oplog.rs']
    ts = Timestamp(config['ts']['time'],config['ts']['inc'])
    sync_time = datetime.datetime.now()
    print('insr sync:', config['sync_table'],'config.ts=',config['ts'], 'timestamp=', ts)
    cursor = cr.find({'ts': {'$gt': ts }}, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True)
    while cursor.alive:
        for doc in cursor:
            config['ts']= doc['ts']
            if get_seconds(sync_time) >= 1:
                write_ckpt(config)
                sync_time = datetime.datetime.now()

            db = doc['ns'].split('.')[0]
            if config['mongo']['db_name'] == db and  config['mongo']['tab_name'] is not None:
                if doc['ns'] in config['sync_table']:
                    if doc['op'] == 'i':
                        msg = dict(config['kafka']['templete'])
                        doc['o']['_id'] = str(doc['o']['_id'] )
                        msg['data'] = [ preprocessor2(doc['o']) ]
                        msg['database'] = doc['ns'].split('.')[0]
                        msg['table'] = doc['ns'].split('.')[1]
                        msg['ts'] = int( round(time.time() * 1000))
                        msg['type'] = 'INSERT'
                        config['producer'].sendjsondata(msg)
                        write_ckpt(config)
                        out = json.dumps(msg,
                                            cls=DateEncoder,
                                            ensure_ascii=False,
                                            indent=4,
                                            separators=(',', ':')) + '\n'
                        if config['sync_settings']['debug'] == 'Y':
                            print('doc=', doc)
                            logging.info(out)

                    if doc['op'] == 'd':
                        msg = dict(config['kafka']['templete'])
                        doc['o']['_id'] = str(doc['o']['_id'])
                        msg['data'] = [ preprocessor2(doc['o']) ]
                        msg['database'] = doc['ns'].split('.')[0]
                        msg['table'] = doc['ns'].split('.')[1]
                        msg['ts'] = int( round(time.time() * 1000))
                        msg['type'] = 'DELETE'
                        config['producer'].sendjsondata(msg)
                        write_ckpt(config)
                        out = json.dumps(msg,
                                            cls=DateEncoder,
                                            ensure_ascii=False,
                                            indent=4,
                                            separators=(',', ':')) + '\n'
                        if config['sync_settings']['debug'] == 'Y':
                            print('doc=', doc)
                            logging.info(out)

                    if doc['op'] == 'u':
                        msg = dict(config['kafka']['templete'])
                        msg['data'] = [ preprocessor2(doc['o']['$set'])] if doc['o'].get('$set', None) is not None else [preprocessor2(doc['o']) ]
                        if doc['o2'].get('_id'):
                           msg['data'][0]['_id'] = str(doc['o2'].get('_id'))
                        msg['database'] = doc['ns'].split('.')[0]
                        msg['table'] = doc['ns'].split('.')[1]
                        msg['ts'] = int( round(time.time() * 1000))
                        msg['type'] = 'UPDATE'
                        config['producer'].sendjsondata(msg)
                        write_ckpt(config)
                        out = json.dumps(msg,
                                            cls=DateEncoder,
                                            ensure_ascii=False,
                                            indent=4,
                                            separators=(',', ':')) + '\n'
                        if config['sync_settings']['debug'] == 'Y':
                            print('doc=',doc)
                            logging.info(out)


            if (config['mongo']['db_name']  is  None or config['mongo']['db_name'] =='') \
                    and (config['mongo']['tab_name'] is None or  config['mongo']['tab_name'] =='') :

                if doc['op'] == 'i':
                    msg = dict(config['kafka']['templete'])
                    doc['o']['_id'] = str(doc['o']['_id'])
                    msg['data'] = [preprocessor2(doc['o'])]
                    msg['database'] = doc['ns'].split('.')[0]
                    msg['table'] = doc['ns'].split('.')[1]
                    msg['ts'] = int(round(time.time() * 1000))
                    msg['type'] = 'INSERT'
                    config['producer'].sendjsondata(msg)
                    write_ckpt(config)
                    out = json.dumps(msg,
                                     cls=DateEncoder,
                                     ensure_ascii=False,
                                     indent=4,
                                     separators=(',', ':')) + '\n'
                    if config['sync_settings']['debug'] == 'Y':
                        print('doc=', doc)
                        logging.info(out)

                if doc['op'] == 'd':
                    msg = dict(config['kafka']['templete'])
                    doc['o']['_id'] = str(doc['o']['_id'])
                    msg['data'] = [preprocessor2(doc['o'])]
                    msg['database'] = doc['ns'].split('.')[0]
                    msg['table'] = doc['ns'].split('.')[1]
                    msg['ts'] = int(round(time.time() * 1000))
                    msg['type'] = 'DELETE'
                    config['producer'].sendjsondata(msg)
                    write_ckpt(config)
                    out = json.dumps(msg,
                                     cls=DateEncoder,
                                     ensure_ascii=False,
                                     indent=4,
                                     separators=(',', ':')) + '\n'
                    if config['sync_settings']['debug'] == 'Y':
                        print('doc=', doc)
                        logging.info(out)

                if doc['op'] == 'u':
                    msg = dict(config['kafka']['templete'])
                    msg['data'] = [preprocessor2(doc['o']['$set'])] if doc['o'].get('$set', None) is not None else [
                        preprocessor2(doc['o'])]
                    if doc['o2'].get('_id'):
                        msg['data'][0]['_id'] = str(doc['o2'].get('_id'))
                    msg['database'] = doc['ns'].split('.')[0]
                    msg['table'] = doc['ns'].split('.')[1]
                    msg['ts'] = int(round(time.time() * 1000))
                    msg['type'] = 'UPDATE'
                    config['producer'].sendjsondata(msg)
                    write_ckpt(config)
                    out = json.dumps(msg,
                                     cls=DateEncoder,
                                     ensure_ascii=False,
                                     indent=4,
                                     separators=(',', ':')) + '\n'
                    if config['sync_settings']['debug'] == 'Y':
                        print('doc=', doc)
                        logging.info(out)

def init_log(config):
    os.system('>{}'.format(config['mongo']['logfile']))

def parse_param():
    parser = argparse.ArgumentParser(description='Resolve mysql binlogfile.')
    parser.add_argument('--conf', help='配置文件', default=None, required=True)
    args = parser.parse_args()
    return args

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def upd_ckpt(cfg):
    ckpt = {
       'ts': cfg['ts'],
       'pid':cfg['pid']
    }
    with open(cfg['ckpt_file'], 'w') as f:
        f.write(json.dumps(ckpt,cls=DateEncoder,ensure_ascii=False, indent=4, separators=(',', ':')))
    logging.info('update ckpt:{}'.format(cfg['ts'] ))
    print('update ckpt:{}'.format(cfg['ts'] ))

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

   # init logger
   logging.basicConfig(
       filename=cfg['SYNC_SETTINGS']['logfile'],
       format='[%(asctime)s-%(levelname)s:%(message)s]',
       level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

   # init config
   config = get_config(cfg)

   # print config
   print_cfg(config)

   # refresh pid
   config['pid'] = os.getpid()
   upd_ckpt(cfg)

   # full sync
   if config['sync_settings']['isInit']:
      full_sync(config)

   # incr sync
   incr_sync(config)

if __name__ == "__main__":
     warnings.filterwarnings("ignore")
     main()

