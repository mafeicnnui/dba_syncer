#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/12/14 11:02
# @Author : 马飞
# @File : sync_mongo_oplog.py
# @Software: PyCharm

import pymongo
import json
import datetime
import sys
import traceback
import argparse
from pymongo.cursor import CursorType
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from bson import ObjectId
from bson import Timestamp

''' ElasticSearch 查询

    1、查询索引下的类型
    curl 10.2.39.129:9200/test?pretty

    2、查询某个文档
    curl '172.17.194.79:9200/test/saleDetail/_search?pretty' -d '
    {
      "query": {
        "bool": {
          "must": [
            {"match":{"_id":"5efc45fa2eec550e40eef153"}}
          ]
        }
      }
    }'
    
    3、查询文档数量
    curl 172.17.194.79:9200/_cat/indices?v
    
    4、查询同步日志
    tail -f sync_mongo2es_oplog.log
    
    5.在ES中查询某个文档
     curl '192.168.100.77:9200/easylife_cs4/estateHouse/6114c036b57bd70001d4080d?pretty'
'''

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

        elif isinstance(obj, ObjectId):
            return str(obj)

        elif isinstance(obj, Timestamp):
            return obj.as_datetime()

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
    conn          = pymongo.MongoClient('mongodb://{0},replicaSet={1}'.format(replstr[0:-1],replset_name))
    db            = conn[service]

    if user!='' and password !='' :
       db.authenticate(user, password)
       db = conn['local']
    return db

'''
    功能：获取ES连接
'''
def get_ds_es(p_ip,p_port):
    conn = Elasticsearch([p_ip],port=p_port)
    return conn

'''
    功能：获取ES连接,用户密码
'''
def get_ds_es_auth_tx(p_ip,p_port,p_user,p_pass):
    conn = Elasticsearch(
        ["https://es-4nokrpqb.public.tencentelasticsearch.com:9200"],
        http_auth=('elastic','21block@2022'),
        sniff_on_start=False,
        sniff_on_connection_fail=False,
        sniffer_timeout=None)
    return conn



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

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

'''
    功能：写同步日志 
'''
def write_log(doc):
    try:
        if doc['op'] =='i' or doc['op'] =='d' :
           doc['o']['_id'] = str(doc['o']['_id'])

        if doc['op']=='u':
           doc['o']['_id'] = str(doc['o']['_id'])
           doc['o2']['_id'] = str(doc['o2']['_id'])

    except:
        print('write_log error:',traceback.print_exc())

    doc=preprocessor(doc)

    with open(MONGO_SETTINGS['logfile'], 'a') as f:
        f.write('Sync time:{},ElasticSearch:index:{},type:{},op:{}\n'.format(get_time(),doc['ns'].split('.')[0],doc['ns'].split('.')[1],doc['op']))
        f.write('_source='+json.dumps(doc, cls=DateEncoder,ensure_ascii=False, indent=4, separators=(',', ':'))+'\n')

    if MONGO_SETTINGS['debug']:
        print('Sync time:{},ElasticSearch:index:{},type:{},op:{}'
              .format(get_time(), doc['ns'].split('.')[0],doc['ns'].split('.')[1], doc['op']))
        print('_source=' + json.dumps(doc, cls=DateEncoder, ensure_ascii=False, indent=4, separators=(',', ':')) + '\n')


'''
    功能：同步所有库数据
'''
def full_sync(cfg):
    cr = cfg['mongo']['oplog.rs']
    first = next(cr.find().sort('$natural', pymongo.DESCENDING).limit(-1))
    ts = first['ts']
    actions = []
    while True:
        cursor = cr.find({'ts': {'$gt': ts}}, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True)
        while cursor.alive:
            for doc in cursor:
                doc['ts'] = str(doc['ts'])
                v_json = {}
                if cfg['MONGO_SETTINGS']['table'] == '' or cfg['MONGO_SETTINGS']['table'] is None:
                    print('full_sync=>doc:=' + json.dumps(doc,
                                                          cls=DateEncoder,
                                                          ensure_ascii=False,
                                                          indent=4,
                                                          separators=(',', ':')) + '\n')
                    print("source doc：",doc)
                    if doc['op'] == 'i':
                        v_json['op']  = 'insert'
                        v_json['obj'] = doc['ns']
                        v_json['val'] = doc['o']
                        id= str(doc['o']['_id'])
                        del doc['o']['_id']
                        es_doc = {
                            "_index" : cfg['ES_SETTINGS']['idx_name'],
                            "_type"  : doc['ns'].split('.')[1],
                            "_id"    : id,
                            # "_source": doc['o']
                            "doc": json.loads(json.dumps(doc['o'],
                                                  cls=DateEncoder,
                                                  ensure_ascii=False,
                                                  indent=4,
                                                  separators=(',', ':')))
                        }
                        actions.append(es_doc)
                        helpers.bulk(cfg['es'], actions)
                        #write_log(doc)
                    elif doc['op'] == 'd':
                        v_json['op'] = 'delete'
                        v_json['obj'] = doc['ns']
                        v_json['val'] = str(doc['o']['_id'])
                        doc['o']['_id'] = v_json['val']
                        es_doc = {
                            "_index": cfg['ES_SETTINGS']['idx_name'],
                            "_type": doc['ns'].split('.')[1],
                            "_id": v_json['val'],
                            # "_source": doc['o'],
                            "_source": json.loads(json.dumps(doc['o'],
                                                  cls=DateEncoder,
                                                  ensure_ascii=False,
                                                  indent=4,
                                                  separators=(',', ':')))
                        }
                        actions.append(es_doc)
                        helpers.bulk(cfg['es'], actions)
                        #write_log(doc)

                    elif doc['op'] == 'u':
                        v_json['op'] = 'update'
                        v_json['obj'] = doc['ns']

                        if doc.get('o2', None) is not None:
                            doc['o2']['_id'] = str(doc['o2']['_id'])

                        if doc['o'].get('$set', None) is not None:
                            v_json['val'] = doc['o']['$set']
                        else:
                            if doc['o'].get('_id') is not None:
                               del doc['o']['_id']
                            v_json['val'] = doc['o']
                            # v_json['val']['_id'] = str(v_json['val']['_id'])

                        es_doc = {
                            "_index" : cfg['ES_SETTINGS']['idx_name'],
                            "_type"  : doc['ns'].split('.')[1],
                            "_id"    : doc['o2']['_id'],
                            # "_source": v_json['val'],
                            "_source": json.loads(json.dumps(v_json['val'],
                                                  cls=DateEncoder,
                                                  ensure_ascii=False,
                                                  indent=4,
                                                  separators=(',', ':')))
                        }
                        actions.append(es_doc)
                        helpers.bulk(cfg['es'], actions)
                        #write_log(doc)
                    else:
                        pass

                    actions = []

'''
    功能：同步某个库下某张表数据
'''
def incr_sync(cfg):
    es = cfg['es']
    cr = cfg['mongo']['oplog.rs']
    first = next(cr.find().sort('$natural', pymongo.DESCENDING).limit(-1))
    ts = first['ts']
    actions =[]
    while True:
        cursor = cr.find({'ts': {'$gt': ts}}, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True)
        while cursor.alive:
            for doc in cursor:
                doc['ts'] = str(doc['ts'])
                v_json = {}
                if len([i for i in cfg['MONGO_SETTINGS']['table'].split(',') if i in doc['ns']])>0:
                    print('doc=', doc)
                    if doc['op'] == 'i':
                        v_json['op'] = 'insert'
                        v_json['obj'] = doc['ns']
                        v_json['val'] = doc['o']
                        id = str(doc['o']['_id'])
                        del doc['o']['_id']
                        print('docstr=',json.loads(json.dumps(doc['o'],
                                                             cls=DateEncoder,
                                                             ensure_ascii=False,
                                                             indent=4,
                                                             separators=(',', ':'))))
                        print('\n')

                        es_doc = {
                            "_index": cfg['MONGO_SETTINGS']['idx_name'],
                            "_type": doc['ns'].split('.')[1],
                            "_id": id,
                            "_source": json.loads(json.dumps(doc['o'],
                                                             cls=DateEncoder,
                                                             ensure_ascii=False,
                                                             indent=4,
                                                             separators=(',', ':')))
                        }
                        actions.append(es_doc)
                        helpers.bulk(cfg['es'], actions)

                    elif doc['op'] == 'd':
                        v_json['op'] = 'delete'
                        v_json['obj'] = doc['ns']
                        v_json['val'] = str(doc['o']['_id'])
                        doc['o']['_id'] = v_json['val']
                        print('docstr=', json.loads(json.dumps(doc['o'],
                                                             cls=DateEncoder,
                                                             ensure_ascii=False,
                                                             indent=4,
                                                             separators=(',', ':'))))
                        print('\n')
                        es.delete(index=cfg['MONGO_SETTINGS']['idx_name'],doc_type= doc['ns'].split('.')[1],id=v_json['val'])
                    elif doc['op'] == 'u':

                        v_json['op'] = 'update'
                        v_json['obj'] = doc['ns']

                        if doc.get('o2', None) is not None:
                            doc['o2']['_id'] = str(doc['o2']['_id'])

                        if doc['o'].get('$set', None) is not None:
                            v_json['val'] = doc['o']['$set']
                        else:
                            if doc['o'].get('_id') is not None:
                                del doc['o']['_id']
                            v_json['val'] = doc['o']

                        print('docstr=', json.loads(json.dumps(v_json['val'],
                                                             cls=DateEncoder,
                                                             ensure_ascii=False,
                                                             indent=4,
                                                             separators=(',', ':'))))
                        print('\n')

                        # 2021.08.30 更新时如果ES中无此文档，则将该object_id数据写入ES后再应用日志
                        try:
                            es.update(index=cfg['MONGO_SETTINGS']['idx_name'],
                                      doc_type= doc['ns'].split('.')[1],
                                      id=doc['o2']['_id'],
                                      body={"doc":json.loads(json.dumps(v_json['val'],
                                                                 cls=DateEncoder,
                                                                 ensure_ascii=False,
                                                                 indent=4,
                                                                 separators=(',', ':')))}
                                      )
                        except:
                            print('Not found doc from  {} get data to es...'.format(doc['ns'].split('.')[1]))
                            if doc.get('o2', None) is not None:
                               res = cfg['mongo'].find({"_id":ObjectId(doc['o2']['_id'])})
                               es_doc = {
                                   "_index": cfg['MONGO_SETTINGS']['idx_name'],
                                   "_type": doc['ns'].split('.')[1],
                                   "_id": doc['o2']['_id'],
                                   "_source": json.loads(json.dumps(res,
                                                                    cls=DateEncoder,
                                                                    ensure_ascii=False,
                                                                    indent=4,
                                                                    separators=(',', ':')))
                               }
                               actions.append(es_doc)
                               helpers.bulk(cfg['es'], actions)

                            print('apply update oplog...for {}'.format(format(doc['ns'].split('.')[1])))
                            es.update(index=cfg['MONGO_SETTINGS']['idx_name'],
                                      doc_type=doc['ns'].split('.')[1],
                                      id=doc['o2']['_id'],
                                      body={"doc": json.loads(
                                           json.dumps(v_json['val'],
                                                      cls=DateEncoder,
                                                      ensure_ascii=False,
                                                      indent=4,
                                                      separators=(',', ':')))}
                                      )
                    else:
                        pass

                    actions = []

'''
    功能：首次同步时在ES上创建索引
'''
def init_es(cfg):
    es=cfg['es']
    '''
         //每张表建一个索引
         for e in MONGO_SETTINGS['table'].split(','):
            es.indices.create(index=e.split('.')[0], ignore=400)
            print('ElasticSearch index {} created!'.format(e.split('.')[0]))
    '''
    # 通过定义索引名创建索引
    es.indices.create(index=cfg['ES_SETTINGS']['index'], ignore=400)
    print('ElasticSearch index {} created!'.format(cfg['ES_SETTINGS']['index']))


'''
    功能：首次同步时在ES上创建索引
'''
def init_es2(cfg):
    es=cfg['es']
    d_mappings= {
        "mappings": {
            "properties": {
            }
        }
    }
    for e in cfg['MONGO_SETTINGS']['table'].split(","):
        db  = e.split('.')[0]
        tab = e.split('.')[1]
        mongo = get_ds_mongo_auth('{0}:{1}:{2}:{3}:{4}'.
                                  format(cfg['MONGO_SETTINGS']['host'],
                                         cfg['MONGO_SETTINGS']['port'],
                                         db,
                                         cfg['MONGO_SETTINGS']['user'],
                                         cfg['MONGO_SETTINGS']['passwd']))

        cur_mongo = mongo[tab]
        results = cur_mongo.find().limit(1)
        print('result=',results)
        col={}
        for cur in results:
            del cur['_id']
            for key in cur:
                if key not in('_id'):
                    col.update({
                        key: {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    })
        d_mappings['mappings']['properties'].update({tab:col})
        print('>>>mappings=',
              json.dumps(d_mappings, cls=DateEncoder, ensure_ascii=False, indent=4, separators=(',', ':')) + '\n')

    print('mappings=',json.dumps(d_mappings, cls=DateEncoder, ensure_ascii=False, indent=4, separators=(',', ':')) + '\n')
    try:
      es.indices.create(index=cfg['ES_SETTINGS']['idx_name'].lower(), ignore=400,body =d_mappings)
      print('ElasticSearch index {} created!'.format(cfg['ES_SETTINGS']['idx_name']))
    except:
      traceback.print_exc()
      print('{} index already exist'.format(cfg['ES_SETTINGS']['idx_name']))


'''
    功能：获取mongo,es连接对象
'''
def get_config(cfg):
    config={}
    # 获取mongo数据库对象
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
    config['mongo'] = mongo

    # 获取ES对象
    es = get_ds_es_auth_tx(cfg['ES_SETTINGS']['host'], cfg['ES_SETTINGS']['port'],cfg['ES_SETTINGS']['user'], cfg['ES_SETTINGS']['passwd'])
    config['es']=es
    return config


'''
    功能：打印配置
'''
def print_cfg(cfg):
    print('Mongodb config:')
    print_dict(cfg['MONGO_SETTINGS'])
    print('\n')
    print('ElasticSearch config:')
    print_dict(cfg['ES_SETTINGS'])

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

   if not cfg['MONGO_SETTINGS']['isSync']:
      print('Sync is disabled!' )
      sys.exit(0)

   # 获取对象
   cfg = get_config(cfg)

   # 输出配置
   print_cfg()

   # 初始化ES
   init_es2(cfg)

   # 数据同步
   if cfg['MONGO_SETTINGS']['table'] == '' or cfg['MONGO_SETTINGS']['table'] is None:
       full_sync(cfg)
   else:
       incr_sync(cfg)


if __name__ == "__main__":
     main()

