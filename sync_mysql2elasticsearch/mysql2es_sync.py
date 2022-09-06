#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/29 9:06
# @Author : 马飞
# @File : mysql2kafka_sync.py.py
# @Software: PyCharm

import os
import sys
import time
import warnings
import json,datetime
import pymysql
import traceback
import argparse
import decimal
import logging
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent)
from pymysqlreplication.event import RotateEvent

def get_db(cfg):
    return get_ds_mysql(cfg['mysql_settings']['host'],cfg['mysql_settings']['port'],'information_schema',cfg['mysql_settings']['user'],cfg['mysql_settings']['passwd'])

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    cfg['cfg_file'] = file
    cfg['ckpt_file'] = file.replace('json','ckpt')
    return cfg

def parse_param():
    parser = argparse.ArgumentParser(description='Resolve mysql binlogfile.')
    parser.add_argument('--conf', help='配置文件', default=None, required=True)
    args = parser.parse_args()
    return args

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        elif isinstance(obj, decimal.Decimal):
            return str(obj)

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

def get_ds_es_auth(p_ip,p_port,p_user,p_pass):
    conn = Elasticsearch(
        ["{}:{}".format(p_ip, p_port)],
        http_auth=(p_user, p_pass),
        sniff_on_start=False,
        sniff_on_connection_fail=False,
        sniffer_timeout=None)
    return conn

def get_ds_es_auth_https(p_ip,p_port,p_user,p_pass):
    conn = Elasticsearch(
        ["https://{}:{}".format(p_ip,p_port)],
        http_auth=(p_user,p_pass),
        sniff_on_start=False,
        sniff_on_connection_fail=False,
        sniffer_timeout=None)
    return conn

def get_master_pos(cfg,file=None,pos=None):
    db = get_db(cfg)
    cr = db.cursor()
    cr.execute('show master status')
    rs=cr.fetchone()
    if file is not None and pos is not None:
        return file,pos
    else:
        return rs[0],rs[1]

def get_file_and_pos(cfg):
    cr = cfg['cr_mysql']
    cr.execute('show master status')
    rs = cr.fetchone()
    return rs

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=False)
    return conn

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=False,cursorclass = pymysql.cursors.DictCursor)
    return conn

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def print_cfg(cfg):
    print('sync config:')
    print_dict(cfg['sync_settings'])
    print('mysql config:')
    print_dict(cfg['mysql_settings'])
    print('\n')
    print('ElasticSearch config:')
    print_dict(cfg['es_settings'])

def check_es_doc_exists(cfg):
   es= cfg['es']
   return es.count(index=cfg['es_settings']['index'],doc_type='_doc')["count"]

def get_sync_table_total_rows(cfg,event):
    cr = cfg['cr_mysql']
    st = "select count(0) from `{0}`.`{1}`".format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchone()
    return  rs[0]

def get_sync_table_pk_names(cfg,event):
    cr = cfg['cr_mysql']
    st="""select column_name  from information_schema.columns
           where table_schema='{0}' and table_name='{1}' and column_key='PRI' order by ordinal_position
       """.format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchall()
    v_col = ''
    for i in list(rs):
        v_col=v_col+i[0]+','
    return v_col[0:-1]

def get_sync_table_cols(cfg,event):
    cr = cfg['cr_mysql']
    st="""select concat('`',column_name,'`') from information_schema.columns
              where table_schema='{0}' and table_name='{1}'  order by ordinal_position
          """.format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchall()
    v_col = ''
    for i in list(rs):
        v_col = v_col + i[0] + ','
    return v_col[0:-1]

def get_sync_table_min_id(cfg,event):
    cr = cfg['cr_mysql']
    cl = get_sync_table_pk_names(cfg,event)
    st = "select min(`{}`) from `{}`.`{}`".format(cl,event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchone()
    return  rs[0] if rs[0]  is not None else 0

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def diff_sync(cfg):
    try:
        stream = BinLogStreamReader(
                     connection_settings = cfg['mysql_settings'],
                     server_id = int(time.time()),
                     blocking  = True,
                     resume_stream = True,
                     log_file = cfg['binlog_file'],
                     log_pos  = int(cfg['binlog_pos']))

        schema = cfg['sync_settings']['db']
        table = cfg['sync_settings']['table'].split(':')[0]
        print('apply diff logs for table:{}.{}...'.format(schema,table))
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
                             "pk_name":get_tab_pk_name(get_db(cfg),binlogevent.schema,binlogevent.table),
                             "id":cfg['sync_settings']['table'].split(':')[1]}

                    if event['schema'] == schema and event['table'] == table :
                        if isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            if cfg['sync_settings']['level'] == '1':
                               if event['id'] is not None and event['id']!='':
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": int(row["values"][event['id']]),
                                        "_source": row["values"]
                                    }
                               else:
                                   es_doc = {
                                       "_index": cfg['es_settings']['index'],
                                       "_id": int(row["values"][event['pk_name']]),
                                       "_source": row["values"],
                                   }
                            elif cfg['sync_settings']['level'] == '2':
                               event[cfg['sync_settings']['table'].split(':')[0]] = row["values"]
                               if event['id'] is not None and event['id']!='':
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": int(row["values"][event['id']]),
                                        "_source": event
                                    }
                               else:
                                   es_doc = {
                                       "_index": cfg['es_settings']['index'],
                                       "_id": int(row["values"][event['pk_name']]),
                                       "_source": event,
                                   }
                            else:
                               logging.info('Not support level!')
                               sys.exit(0)

                            helpers.bulk(cfg['es'], [es_doc])
                            write_ckpt(cfg)
                            if cfg['sync_settings']['debug'] == 'Y':
                                logging.info('insert:'+json.dumps(es_doc,
                                                                   cls=DateEncoder,
                                                                   ensure_ascii=False,
                                                                   indent=4,
                                                                   separators=(',', ':')) + '\n')
                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            if cfg['sync_settings']['level'] == '1':
                                if event['id'] is not None and event['id']!='':
                                    id = int(row["after_values"][event['id']])
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": id,
                                        "_source": row["after_values"],
                                    }
                                else:
                                    id = int(row["after_values"][event['pk_name']])
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": id,
                                        "_source": row["after_values"],
                                    }
                            elif cfg['sync_settings']['level'] == '2':
                                event[cfg['sync_settings']['table'].split(':')[0]] = row["after_values"]
                                if event['id'] is not None and event['id']!='':
                                    id = int(row["after_values"][event['id']])
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": int(row["after_values"][event['id']]),
                                        "_source": event,
                                    }
                                else:
                                    id = int(row["after_values"][event['pk_name']])
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": id,
                                        "_source": event,
                                    }
                            else:
                                logging.info('Not support level!')
                                sys.exit(0)

                            try:
                               cfg['es'].update(index=cfg['es_settings']['index'],doc_type='_doc' ,id=id, body={"doc":event})
                               write_ckpt(cfg)
                               if cfg['sync_settings']['debug'] == 'Y':
                                   logging.info('update:'+json.dumps(es_doc,
                                                                      cls=DateEncoder,
                                                                      ensure_ascii=False,
                                                                      indent=4,
                                                                      separators=(',', ':')) + '\n')
                            except:
                               print('Doc not found,update failure!')
                        elif isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            if cfg['sync_settings']['level'] == '1':
                                if event['id'] is not None and event['id']!='':
                                   id = int(row["values"][event['id']])
                                else:
                                   id = int(row["values"][event['pk_name']])
                            elif cfg['sync_settings']['level'] == '2':
                                if event['id'] is not None and event['id']!='':
                                    id = int(row["values"][event['id']])
                                else:
                                    id = int(row["values"][event['pk_name']])
                            else:
                                logging.info('Not support level!')
                                sys.exit(0)

                            event[cfg['sync_settings']['table']] = row["values"]
                            delete_by_id = {
                                "query": {"match":{"_id":id }}
                            }
                            try:
                               cfg['es'].delete_by_query(index=cfg['es_settings']['index'],body=delete_by_id,doc_type='_doc')
                               write_ckpt(cfg)
                               if cfg['sync_settings']['debug'] == 'Y':
                                   logging.info('delete table:{},id:{}'.format(event['table'], delete_by_id))
                            except:
                               logging.info('Doc not found,delete failure!')

            if stream.log_file == cfg['full_binlog_file'] and (
                    stream.log_pos + 31 == int(cfg['full_binlog_pos']) or stream.log_pos >= int(
                    cfg['full_binlog_pos'])):
                stream.close()
                break

    except Exception as e:
        logging.info(traceback.print_exc())

def incr_sync(cfg):
    try:
        stream = BinLogStreamReader(
                     connection_settings = cfg['mysql_settings'],
                     server_id = int(time.time()),
                     blocking  = True,
                     resume_stream = True,
                     log_file = cfg['full_binlog_file'],
                     log_pos  = int(cfg['full_binlog_pos']))

        schema = cfg['sync_settings']['db']
        table = cfg['sync_settings']['table'].split(':')[0]
        sync_time = datetime.datetime.now()
        print('apply incr logs for table:{}.{}...'.format(schema, table))
        for binlogevent in stream:
            cfg['binlog_file'] = stream.log_file
            cfg['binlog_pos'] = stream.log_pos

            if get_seconds(sync_time) >= 1:
               write_ckpt(cfg)
               sync_time = datetime.datetime.now()

            if isinstance(binlogevent, RotateEvent):
                cfg['binlog_file'] = binlogevent.next_binlog
                logging.info("\033[1;34;40mbinlog file has changed!\033[0m")

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
                             "pk_name": get_tab_pk_name(get_db(cfg), binlogevent.schema, binlogevent.table),
                             "id": cfg['sync_settings']['table'].split(':')[1]}

                    if event['schema'] == schema and event['table'] == table :
                        if isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            if cfg['sync_settings']['level'] == '1':
                                if event['id'] is not None and event['id']!='':
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": int(row["values"][event['id']]),
                                        "_source": row["values"]
                                    }
                                else:
                                   es_doc = {
                                       "_index": cfg['es_settings']['index'],
                                       "_id": int(row["values"][event['pk_name']]),
                                       "_source": row["values"],
                                   }
                            elif cfg['sync_settings']['level'] == '2':
                                event[cfg['sync_settings']['table'].split(':')[0]] = row["values"]
                                if event['id'] is not None and event['id']!='':
                                    es_doc = {
                                       "_index": cfg['es_settings']['index'],
                                       "_id": int(row["values"][event['id']]),
                                       "_source": event
                                    }
                                else:
                                    es_doc = {
                                       "_index": cfg['es_settings']['index'],
                                       "_id": int(row["values"][event['pk_name']]),
                                       "_source": event,
                                    }
                            else:
                               logging.info('Not support level!')
                               sys.exit(0)

                            if cfg['sync_settings']['debug'] == 'Y':
                                logging.info('insert:'+json.dumps(es_doc,
                                                                  cls=DateEncoder,
                                                                  ensure_ascii=False,
                                                                  indent=4,
                                                                  separators=(',', ':')) + '\n')
                            helpers.bulk(cfg['es'], [es_doc])
                            write_ckpt(cfg)
                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            if cfg['sync_settings']['level'] == '1':
                                if event['id'] is not None and event['id']!='':
                                    id = int(row["after_values"][event['id']])
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": id,
                                        "_source": row["after_values"],
                                    }
                                else:
                                    id = int(row["after_values"][event['pk_name']])
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": id,
                                        "_source": row["after_values"],
                                    }
                            elif cfg['sync_settings']['level'] == '2':
                                event[cfg['sync_settings']['table'].split(':')[0]] = row["after_values"]
                                if event['id'] is not None and event['id']!='':
                                    id = int(row["after_values"][event['id']])
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": int(row["after_values"][event['id']]),
                                        "_source": event,
                                    }
                                else:
                                    id = int(row["after_values"][event['pk_name']])
                                    es_doc = {
                                        "_index": cfg['es_settings']['index'],
                                        "_id": id,
                                        "_source": event,
                                    }
                            else:
                                logging.info('Not support level!')
                                sys.exit(0)

                            try:
                               if cfg['sync_settings']['level'] == '1':
                                  cfg['es'].update(index=cfg['es_settings']['index'],doc_type='_doc' ,id=id, body={"doc":row["after_values"]})
                               if cfg['sync_settings']['level'] == '2':
                                  cfg['es'].update(index=cfg['es_settings']['index'],doc_type='_doc' ,id=id, body={"doc":event})
                               write_ckpt(cfg)
                               if cfg['sync_settings']['debug'] == 'Y':
                                  logging.info('update:'+json.dumps(es_doc,
                                                              cls=DateEncoder,
                                                              ensure_ascii=False,
                                                              indent=4,
                                                              separators=(',', ':')) + '\n')

                            except:
                               logging.info(traceback.format_exc())

                        elif isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            if cfg['sync_settings']['level'] == '1':
                                if event['id'] is not None and event['id']!='':
                                    id = int(row["values"][event['id']])
                                else:
                                    id = int(row["values"][event['pk_name']])
                            elif cfg['sync_settings']['level'] == '2':
                                if event['id'] is not None and event['id']!='':
                                    id = int(row["values"][event['id']])
                                else:
                                    id = int(row["values"][event['pk_name']])
                            else:
                                print('Not support level!')
                                sys.exit(0)

                            event[cfg['sync_settings']['table']] = row["values"]
                            delete_by_id = {
                                "query": {"match":{"_id":id }}
                            }
                            try:
                               cfg['es'].delete_by_query(index=cfg['es_settings']['index'],body=delete_by_id,doc_type='_doc')
                               write_ckpt(cfg)
                               if cfg['sync_settings']['debug'] == 'Y':
                                   logging.info('delete table:{},id:{}'.format(event['table'], delete_by_id))
                            except:
                               logging.info(traceback.format_exc())


    except Exception as e:
        traceback.print_exc()

def full_sync(cfg):
    cfg['cr_mysql_dict'].execute('FLUSH /*!40101 LOCAL */ TABLES')
    cfg['cr_mysql_dict'].execute('FLUSH TABLES WITH READ LOCK')
    cfg['cr_mysql_dict'].execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    cfg['cr_mysql_dict'].execute('START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */')
    cfg['full_binlog_file'], cfg['full_binlog_pos'] = get_file_and_pos(cfg)[0:2]
    logging.info('full sync checkpoint:{}/{}'.format(cfg['full_binlog_file'], cfg['full_binlog_pos']))
    cfg['cr_mysql_dict'].execute('UNLOCK TABLES')
    event = {'schema': cfg['sync_settings']['db'],
             'table': cfg['sync_settings']['table'].split(':')[0],
             'id':cfg['sync_settings']['table'].split(':')[1]}
    logging.info('full sync table:{}.{}...'.format(event['schema'],event['table']))
    print('full sync table:{}.{}...'.format(event['schema'],event['table']))
    if  check_es_doc_exists(cfg) == 0:
        i_counter = 0
        n_tab_total_rows = get_sync_table_total_rows(cfg, event)
        v_pk_col_name    = get_sync_table_pk_names(cfg, event)
        v_sync_table_cols = get_sync_table_cols(cfg, event)
        n_batch_size = int(cfg['sync_settings']['batch'])
        n_row = get_sync_table_min_id(cfg, event)
        cr = cfg['cr_mysql_dict']
        while n_tab_total_rows > 0:
            st = "select {} from `{}`.`{}` where {} between {} and {}" \
                .format(v_sync_table_cols, event['schema'],event['table'], v_pk_col_name, str(n_row),str(n_row + n_batch_size))
            if cfg['sync_settings']['debug'] == 'Y':
               logging.info('sql:'+st)
            cr.execute(st)
            rs = cr.fetchall()
            batch = []
            if len(rs) > 0:
                for i in rs:
                   if cfg['sync_settings']['level'] == '1':
                       if event['id'] is not None and event['id']!='':
                           doc = {
                                "_index": cfg['es_settings']['index'],
                                 "_id": int(i[event['id']]),
                                "_source": i
                           }
                       else:
                           doc = {
                               "_index": cfg['es_settings']['index'],
                               "_id": int(i[v_pk_col_name]),
                               "_source": i
                           }
                   elif cfg['sync_settings']['level'] == '2':
                       evt = {}
                       evt[event['table']] = i
                       if event['id'] is not None and event['id']!='':
                           doc = {
                               "_index": cfg['es_settings']['index'],
                               "_id": int(i[event['id']]),
                               "_source": evt
                           }
                       else:
                           doc = {
                               "_index": cfg['es_settings']['index'],
                               "_id": int(i[v_pk_col_name]),
                               "_source": evt
                           }
                   else:
                       logging.info('Not support level!')
                       sys.exit(0)

                   batch.append(doc)
            if len(batch) >0:
               helpers.bulk(cfg['es'], batch)
               if cfg['sync_settings']['debug'] == 'Y':
                  logging.info(json.dumps(batch,
                                          cls=DateEncoder,
                                          ensure_ascii=False,
                                          indent=4,
                                          separators=(',', ':')) + '\n')
               logging.info('full sync es {} records!'.format(len(batch)))
            i_counter = i_counter + len(rs)
            n_row = n_row + n_batch_size + 1
            if i_counter >= n_tab_total_rows or n_tab_total_rows == 0:
                break
    cfg['db_mysql_dict'].commit()

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

def get_binlog_files(cfg):
    cr = cfg['cr_mysql']
    cr.execute('show binary logs')
    files = []
    rs = cr.fetchall()
    for r in rs:
        files.append(r[0])
    return files

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def upd_ckpt(cfg):
    ckpt = {
       'binlog_file': cfg['binlog_file'],
       'binlog_pos' : cfg['binlog_pos'],
       'last_update_time': get_time(),
       'pid':cfg['pid']
    }
    with open(cfg['ckpt_file'], 'w') as f:
        f.write(json.dumps(ckpt, ensure_ascii=False, indent=4, separators=(',', ':')))
    logging.info('update ckpt:{}/{}'.format(cfg['binlog_file'],cfg['binlog_pos'] ))
    print('update ckpt:{}/{}'.format(cfg['binlog_file'],cfg['binlog_pos'] ))

def write_ckpt(cfg):
    ck = {
       'binlog_file': cfg['binlog_file'],
       'binlog_pos' : cfg['binlog_pos'],
       'last_update_time': get_time(),
       'pid':os.getpid()
    }
    with open(cfg['ckpt_file'], 'w') as f:
        f.write(json.dumps(ck, ensure_ascii=False, indent=4, separators=(',', ':')))
    logging.info('update checkpoint:{}/{}'.format(cfg['binlog_file'],cfg['binlog_pos'] ))

def get_config(cfg):
    config=cfg
    config['sync_settings'] = cfg['SYNC_SETTINGS']
    config['mysql_settings'] = cfg['MYSQL_SETTINGS']
    config['es_settings'] = cfg['ES_SETTINGS']
    config['db_mysql'] = get_ds_mysql(cfg['MYSQL_SETTINGS']['host'],
                                      cfg['MYSQL_SETTINGS']['port'],
                                      cfg['SYNC_SETTINGS']['db'],
                                      cfg['MYSQL_SETTINGS']['user'],
                                      cfg['MYSQL_SETTINGS']['passwd'])
    config['cr_mysql'] = config['db_mysql'].cursor()
    config['db_mysql_dict'] = get_ds_mysql_dict(cfg['MYSQL_SETTINGS']['host'],
                                                cfg['MYSQL_SETTINGS']['port'],
                                                cfg['SYNC_SETTINGS']['db'],
                                                cfg['MYSQL_SETTINGS']['user'],
                                                cfg['MYSQL_SETTINGS']['passwd'])
    config['cr_mysql_dict'] = config['db_mysql_dict'].cursor()
    if config['es_settings']['schema'] == 'https':
       config['es'] = get_ds_es_auth_https(cfg['ES_SETTINGS']['host'], cfg['ES_SETTINGS']['port'],cfg['ES_SETTINGS']['user'], cfg['ES_SETTINGS']['passwd'])
    else:
       config['es'] = get_ds_es_auth(cfg['ES_SETTINGS']['host'], cfg['ES_SETTINGS']['port'],cfg['ES_SETTINGS']['user'], cfg['ES_SETTINGS']['passwd'])
    config['sync_table'] = ['{}.{}'.format(cfg['SYNC_SETTINGS'].get('db"'),t) for t in cfg['SYNC_SETTINGS'].get('table').split(',')]

    if check_ckpt(cfg):
        ckpt = read_ckpt(cfg)
        file = ckpt['binlog_file']
        pos = ckpt['binlog_pos']
        pid = ckpt['pid']
        if file not in get_binlog_files(config):
            logging.info('binlog file not exist mysql server,get current file,pos!!!')
            file, pos = get_file_and_pos(config)[0:2]
    else:
        file, pos = get_file_and_pos(config)[0:2]
        pid = os.getpid()

    config['binlog_file'] = file
    config['binlog_pos'] = pos
    config['pid'] = pid
    upd_ckpt(config)
    return config

def create_index(cfg):
    try:
        cfg['es'].indices.create(index=cfg['es_settings']['index'].lower(), body=cfg['es_settings']['mapping'])
        print('ElasticSearch index {} created!'.format(cfg['es_settings']['index'].lower()))
    except:
        print('{} index already exist,skip!'.format(cfg['es_settings']['index'].lower()))

def main():
    # read config
    args = parse_param()
    cfg = read_json(args.conf)

    if not cfg['SYNC_SETTINGS']['isSync']:
        print('Sync is disabled!')
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

    # create index
    create_index(config)

    # full sync
    full_sync(config)

    # diff sync
    diff_sync(config)

    # incr sync
    incr_sync(config)


if __name__ == "__main__":
     warnings.filterwarnings("ignore")
     main()
