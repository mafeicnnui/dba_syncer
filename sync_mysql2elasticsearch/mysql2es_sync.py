#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/29 9:06
# @Author : 马飞
# @File : mysql2kafka_sync.py.py
# @Software: PyCharm

import json,datetime
import pymysql
import traceback
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent)

'''
    curl 10.2.39.41:9200/test?pretty
    curl 10.2.39.41:9200/test/xs/35?pretty
    curl 10.2.39.41:9200/test/xs/_search?pretty
    curl 10.2.39.41:9200/test/xs/_mapping?pretty
    curl '10.2.39.41:9200/test/xs/_search?pretty' -d '
    {
      "query": {
        "bool": {
          "must": [
            {"term":{"data.xh":3}}
          ]
        }
      }
    }'
    
    curl '10.2.39.41:9200/test/xs/_search?pretty' -d '
    {
      "query": {
        "bool": {
          "must": [
            {"term":{"data.xm":"zhang.san"}}
          ]
        }
      }
    }'
    
    curl '10.2.39.41:9200/test/xs/_search?pretty' -d '
    {
      "query": {
        "bool": {
          "must": [
            {"match":{"data.xm":"zhang.san"}}
          ]
        }
      }
    }'
    curl '10.2.39.41:9200/test/xs/_search?pretty' -d '
    {
      "query": {
        "bool": {
          "must": [
            {"match":{"after_values.xh":35}}
          ]
        }
      }
    }'

'''
MYSQL_SETTINGS = {
    "host"  : "10.2.39.18",
    "port"  : 3306,
    "user"  : "puppet",
    "passwd": "Puppet@123",
    "db"    : "test"
}

ES_SETTINGS = {
    "host"  : '10.2.39.41',
    "port"  :  9200,
    "index" :  "test",
    "batch" : 3
}

class DateEncoder(json.JSONEncoder):
    '''
      自定义类，解决报错：
      TypeError: Object of type 'datetime' is not JSON serializable
    '''
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        else:
            return json.JSONEncoder.default(self, obj)

def get_event_name(event):
    if event==2:
       return 'QueryEvent'.ljust(20,' ')+':'
    elif event==30:
       return 'WriteRowsEvent'.ljust(20,' ')+':'
    elif event==31:
       return 'UpdateRowsEvent'.ljust(20,' ')+':'
    elif event==32:
       return 'DeleteRowsEvent'.ljust(20,' ')+':'
    else:
       return ''.ljust(30,' ')

def get_tab_pk_name(db,schema,table):
    cr = db.cursor()
    st = """select column_name 
                from information_schema.columns
                where table_schema='{}'
                  and table_name='{}' and column_key='PRI' order by ordinal_position
            """.format(schema,table)
    cr.execute(st)
    rs = cr.fetchall()
    v = ''
    for i in list(rs):
        v = v + i[0] + ','
    db.commit()
    cr.close()
    return v[0:-1]
    pass

def get_ds_es(p_ip,p_port):
    conn = Elasticsearch([p_ip],port=p_port)
    return conn

def get_master_pos(file=None,pos=None):
    db = get_db(MYSQL_SETTINGS)
    cr = db.cursor()
    cr.execute('show master status')
    rs=cr.fetchone()
    if file is not None and pos is not None:
        return file,pos
    else:
        return rs[0],rs[1]

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=True)
    return conn

def get_db(config):
    return get_ds_mysql(config['host'],config['port'],config['db'],config['user'],config['passwd'])

'''
  规则：1.表无主键不允许同步
'''
def main():
    try:
        file,pos=get_master_pos()
        stream = BinLogStreamReader(
                     connection_settings = MYSQL_SETTINGS,
                     server_id = 8,
                     blocking  = True,
                     resume_stream = True,
                     log_file = file,
                     log_pos  = int(pos)
        )

        es = get_ds_es(ES_SETTINGS['host'], ES_SETTINGS['port'])
        schema = MYSQL_SETTINGS['db']
        actions = []
        for binlogevent in stream:
            if binlogevent.event_type in (2,):
                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                if 'create' in event['query'] or 'drop' in event['query'] \
                        or 'alter' in event['query'] or 'truncate' in event['query']:
                    if event['schema'] == schema:
                       print(event)
            if binlogevent.event_type in (30, 31, 32):
                for row in binlogevent.rows:
                    event = {"schema": binlogevent.schema,
                             "table": binlogevent.table,
                             "pk_name":get_tab_pk_name(get_db(MYSQL_SETTINGS),binlogevent.schema,binlogevent.table)}

                    if event['schema'] == schema:
                        if isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            event["data"] = row["values"]
                            es_doc = {
                                "_index": ES_SETTINGS['index'],
                                "_type" : event['table'],
                                "_id"   : int(event['data'][event['pk_name']]),
                                "_source": event,
                            }
                            actions.append(es_doc)
                            helpers.bulk(es, actions)
                            actions = []
                            print(es_doc)
                            print('elasticsearch insert event apply ok!')
                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            event["data"] = row["after_values"]
                            es_doc = {
                                "_index": ES_SETTINGS['index'],
                                "_type": event['table'],
                                "_id": int(event['data'][event['pk_name']]),
                                "_source": event,
                            }
                            actions.append(es_doc)
                            helpers.bulk(es, actions)
                            actions = []
                            print(es_doc)
                            print('elasticsearch update event apply ok!')

                        elif isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["data"] = row["values"]
                            delete_by_id = {
                                "query": {"match":{"_id":int(event['data'][event['pk_name']])}}
                            }
                            print('delete {},query={}'.format(event['table'],delete_by_id))
                            es.delete_by_query(index=ES_SETTINGS['index'],body=delete_by_id,doc_type=event['table'])
                            print('elasticsearch delete event apply ok!')

                        # if len(actions) % ES_SETTINGS['batch'] == 0:
                        #     print('write event into elasticsearch!')
                        #     helpers.bulk(es, actions)
                        #     actions=[]

    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    main()