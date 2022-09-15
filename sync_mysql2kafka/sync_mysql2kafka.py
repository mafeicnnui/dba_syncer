#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/29 9:06
# @Author : 马飞
# @File : mysql2kafka_sync.py.py
# @Software: PyCharm

import os
import sys
import json
import time
import datetime
import logging
import pymysql
import decimal
import traceback
import argparse
from kafka  import KafkaProducer
from kafka.errors import KafkaError
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent)

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return json.JSONEncoder.default(self, obj)

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def print_cfg(config):
    print('sync settings:')
    print_dict(config['sync_settings'])
    print('mysql config:')
    print_dict(config['mysql_settings'])
    print('Kafka config:')
    print_dict(config['kafka'])

def parse_param():
    parser = argparse.ArgumentParser(description='Resolve mysql binlogfile.')
    parser.add_argument('--conf', help='配置文件', default=None, required=True)
    args = parser.parse_args()
    return args

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    cfg['cfg_file'] = file
    cfg['ckpt_file'] = file.replace('json', 'ckpt')
    return cfg

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
            parmas_message = json.dumps(params, cls=DateEncoder,ensure_ascii=False)
            producer = self.producer
            producer.send(self.kafkatopic, parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(str(e))

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

def get_master_pos(cfg,file=None,pos=None):
    db = get_db(cfg)
    cr = db.cursor()
    cr.execute('show master status')
    rs=cr.fetchone()
    if file is not None and pos is not None:
        return file,pos
    else:
        return rs[0],rs[1]

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=False)
    return conn

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=False,cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_db(cfg):
    return get_ds_mysql(cfg['mysql_settings']['host'],
                        cfg['mysql_settings']['port'],
                        cfg['sync_settings']['db'],
                        cfg['mysql_settings']['user'],
                        cfg['mysql_settings']['passwd'])

def get_table_pk_names(cfg,event):
    cr = cfg['cr_mysql']
    v_col=[]
    v_sql="""select column_name 
              from information_schema.columns
              where table_schema='{}'
                and table_name='{}' and column_key='PRI' order by ordinal_position
          """.format(event['schema'],event['table'])
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in list(rs):
        v_col.append(i[0])
    return v_col

def get_file_and_pos(cfg):
    cr = cfg['cr_mysql']
    cr.execute('show master status')
    rs = cr.fetchone()
    return rs

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
    return rs[0] if rs[0]  is not None else 0

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def check_ckpt(cfg):
    return os.path.isfile(cfg['ckpt_file'])

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

def read_ckpt(cfg):
    with open(cfg['ckpt_file'], 'r') as f:
        contents = f.read()
    if  contents == '':
        return ''
    else:
        binlog = json.loads(contents)
        return binlog

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_binlog_files(cfg):
    cr = cfg['cr_mysql']
    cr.execute('show binary logs')
    files = []
    rs = cr.fetchall()
    for r in rs:
        files.append(r[0])
    return files

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

def full_sync(cfg):
    cfg['cr_mysql_dict'].execute('FLUSH /*!40101 LOCAL */ TABLES')
    cfg['cr_mysql_dict'].execute('FLUSH TABLES WITH READ LOCK')
    cfg['cr_mysql_dict'].execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    cfg['cr_mysql_dict'].execute('START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */')
    cfg['full_binlog_file'], cfg['full_binlog_pos'] = get_file_and_pos(cfg)[0:2]
    logging.info('full sync checkpoint:{}/{}'.format(cfg['full_binlog_file'], cfg['full_binlog_pos']))
    cfg['cr_mysql_dict'].execute('UNLOCK TABLES')
    producer = cfg['producer']
    for tab in cfg['sync_settings']['table'].split(','):
        event = {'schema': cfg['sync_settings']['db'], 'table': tab}
        logging.info('full sync table:{}.{}...'.format(event['schema'],event['table']))
        print('full sync table:{}.{}...'.format(event['schema'],event['table']))
        if  cfg['sync_settings']['isInit']:
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
                logging.info('sql:'+st)
                cr.execute(st)
                rs = cr.fetchall()
                if len(rs) > 0:
                    msg = dict(cfg['kafka']['templete'])
                    msg['data'] = rs
                    msg['database'] = event['schema']
                    msg['dataType'] = 'FULL'
                    msg['pkNames'] = get_table_pk_names(cfg, event)
                    msg['table'] = event['table']
                    msg['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                    msg['type'] = 'INSERT'
                    out = json.dumps(msg,
                                        cls=DateEncoder,
                                        ensure_ascii=False,
                                        indent=4,
                                        separators=(',', ':')) + '\n'
                    if cfg['sync_settings']['debug'] == 'Y':
                        logging.info(out)
                    producer.sendjsondata(msg)

                i_counter = i_counter + len(rs)
                print("Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                             format(tab, n_tab_total_rows, i_counter,
                                    round(i_counter / n_tab_total_rows * 100, 2)))

                n_row = n_row + n_batch_size + 1
                if i_counter >= n_tab_total_rows or n_tab_total_rows == 0:
                    break
    cfg['db_mysql_dict'].commit()

def diff_sync(cfg):
    try:
        stream = BinLogStreamReader(
                     connection_settings = cfg['mysql_settings'],
                     server_id  = int(time.time()),
                     blocking   = True,
                     resume_stream = True,
                     log_file = cfg['binlog_file'],
                     log_pos  = int(cfg['binlog_pos']))

        producer = cfg['producer']
        schema = cfg['sync_settings']['db']
        table = cfg['sync_settings']['table'].split(',')
        print('apply diff logs for table:{}.{}...'.format(schema, table))
        for binlogevent in stream:
            if binlogevent.event_type in (2,):
                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                if 'create' in event['query'] or 'drop' in event['query'] \
                        or 'alter' in event['query'] or 'truncate' in event['query']:
                    if event['schema'] == schema:
                        producer.sendjsondata(event)
                        if cfg['sync_settings']['debug'] == 'Y':
                            print(event)
                            logging.info(json.dumps(event,
                                                    cls=DateEncoder,
                                                    ensure_ascii=False,
                                                    indent=4,
                                                    separators=(',', ':')) + '\n')

            if binlogevent.event_type in (30, 31, 32):
                for row in binlogevent.rows:
                    event = {"schema": binlogevent.schema, "table": binlogevent.table}
                    if event['schema'] == schema:
                        if cfg['sync_settings']['debug'] == 'Y':
                            print(event)
                            logging.info(json.dumps(event,
                                                    cls=DateEncoder,
                                                    ensure_ascii=False,
                                                    indent=4,
                                                    separators=(',', ':')) + '\n')

                        if event['schema'] == schema and event['table'] in table:
                            if  isinstance(binlogevent, WriteRowsEvent):
                                msg = dict(cfg['kafka']['templete'])
                                msg['data'] = [row["values"]]
                                msg['database'] = event['schema']
                                msg['pkNames'] = get_table_pk_names(cfg,event)
                                msg['table'] = event['table']
                                msg['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                                msg['type'] = 'INSERT'
                                producer.sendjsondata(msg)
                                if cfg['sync_settings']['debug'] == 'Y':
                                    log = json.dumps(msg,
                                                     cls=DateEncoder,
                                                     ensure_ascii=False,
                                                     indent=4,
                                                     separators=(',', ':')) + '\n'
                                    logging.info(log)

                            elif isinstance(binlogevent, UpdateRowsEvent):
                                msg = dict(cfg['kafka']['templete'])
                                msg['data'] = [row["after_values"]]
                                msg['old'] = [row["before_values"]]
                                msg['database'] = event['schema']
                                msg['pkNames'] = get_table_pk_names(cfg, event)
                                msg['table'] = event['table']
                                msg['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                                msg['type'] = 'UPDATE'
                                producer.sendjsondata(msg)
                                if cfg['sync_settings']['debug'] == 'Y':
                                    log = json.dumps(msg,
                                                     cls=DateEncoder,
                                                     ensure_ascii=False,
                                                     indent=4,
                                                     separators=(',', ':')) + '\n'
                                    logging.info(log)
                            elif isinstance(binlogevent, DeleteRowsEvent):
                                msg = dict(cfg['kafka']['templete'])
                                msg['data'] = [row["values"]]
                                msg['database'] = event['schema']
                                msg['pkNames'] = get_table_pk_names(cfg, event)
                                msg['table'] = event['table']
                                msg['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                                msg['type'] = 'DELETE'
                                producer.sendjsondata(msg)
                                if cfg['sync_settings']['debug'] == 'Y':
                                    log = json.dumps(msg,
                                                     cls=DateEncoder,
                                                     ensure_ascii=False,
                                                     indent=4,
                                                     separators=(',', ':')) + '\n'
                                    logging.info(log)

            if stream.log_file == cfg['full_binlog_file'] and (
                    stream.log_pos + 31 == int(cfg['full_binlog_pos']) or stream.log_pos >= int(
                cfg['full_binlog_pos'])):
                stream.close()
                break

    except Exception as e:
        traceback.print_exc()

def incr_sync(cfg):
    try:
        stream = BinLogStreamReader(
                     connection_settings = cfg['mysql_settings'],
                     server_id  = int(time.time()),
                     blocking   = True,
                     resume_stream = True,
                     log_file = cfg['full_binlog_file'],
                     log_pos  = int(cfg['full_binlog_pos']))

        producer = cfg['producer']
        schema = cfg['sync_settings']['db']
        table = cfg['sync_settings']['table'].split(',')
        sync_time = datetime.datetime.now()
        print('apply incr logs for table:{}.{}...'.format(schema, table))
        for binlogevent in stream:
            cfg['binlog_file'] = stream.log_file
            cfg['binlog_pos'] = stream.log_pos

            if get_seconds(sync_time) >= 1:
                write_ckpt(cfg)
                sync_time = datetime.datetime.now()

            if binlogevent.event_type in (2,):
                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                if 'create' in event['query'] or 'drop' in event['query'] \
                        or 'alter' in event['query'] or 'truncate' in event['query']:
                    if event['schema'] == schema:
                        producer.sendjsondata(event)
                        if cfg['sync_settings']['debug'] == 'Y':
                            logging.info(json.dumps(event,
                                                    cls=DateEncoder,
                                                    ensure_ascii=False,
                                                    indent=4,
                                                    separators=(',', ':')) + '\n')

            if binlogevent.event_type in (30, 31, 32):
                for row in binlogevent.rows:
                    event = {"schema": binlogevent.schema, "table": binlogevent.table}
                    if event['schema'] == schema:
                        if cfg['sync_settings']['debug'] == 'Y':
                            logging.info(json.dumps(event,
                                                    cls=DateEncoder,
                                                    ensure_ascii=False,
                                                    indent=4,
                                                    separators=(',', ':')) + '\n')

                        if event['schema'] == schema and event['table'] in table:
                            if  isinstance(binlogevent, WriteRowsEvent):
                                msg = dict(cfg['kafka']['templete'])
                                msg['data'] = [row["values"]]
                                msg['database'] = event['schema']
                                msg['pkNames'] = get_table_pk_names(cfg,event)
                                msg['table'] = event['table']
                                msg['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                                msg['type'] = 'INSERT'
                                if cfg['sync_settings']['debug'] == 'Y':
                                    logging.info(json.dumps(msg,
                                                            cls=DateEncoder,
                                                            ensure_ascii=False,
                                                            indent=4,
                                                            separators=(',', ':')) + '\n')
                                producer.sendjsondata(msg)
                                write_ckpt(cfg)
                            elif isinstance(binlogevent, UpdateRowsEvent):
                                msg = dict(cfg['kafka']['templete'])
                                msg['data'] = [row["after_values"]]
                                msg['old'] = [row["before_values"]]
                                msg['database'] = event['schema']
                                msg['pkNames'] = get_table_pk_names(cfg, event)
                                msg['table'] = event['table']
                                msg['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                                msg['type'] = 'UPDATE'
                                if cfg['sync_settings']['debug'] == 'Y':
                                    logging.info(json.dumps(msg,
                                                            cls=DateEncoder,
                                                            ensure_ascii=False,
                                                            indent=4,
                                                            separators=(',', ':')) + '\n')
                                producer.sendjsondata(msg)
                                write_ckpt(cfg)
                            elif isinstance(binlogevent, DeleteRowsEvent):
                                msg = dict(cfg['kafka']['templete'])
                                msg['data'] = [row["values"]]
                                msg['database'] = event['schema']
                                msg['pkNames'] = get_table_pk_names(cfg, event)
                                msg['table'] = event['table']
                                msg['ts'] = int(time.mktime(datetime.datetime.now().timetuple()))
                                msg['type'] = 'DELETE'
                                if cfg['sync_settings']['debug'] == 'Y':
                                    logging.info(json.dumps(msg,
                                                            cls=DateEncoder,
                                                            ensure_ascii=False,
                                                            indent=4,
                                                            separators=(',', ':')) + '\n')
                                producer.sendjsondata(msg)
                                write_ckpt(cfg)


    except Exception as e:
        traceback.print_exc()

def get_config(cfg):
    config=cfg
    producer = Kafka_producer(cfg['KAFKA_SETTINGS']['host'].split(','), cfg['KAFKA_SETTINGS']['port'].split(','), cfg['KAFKA_SETTINGS']['topic'])
    config['producer'] = producer
    config['kafka'] = cfg['KAFKA_SETTINGS']
    config['sync_settings'] = cfg['SYNC_SETTINGS']
    config['mysql_settings'] = cfg['MYSQL_SETTINGS']
    #config['sync_table'] = ['{}.{}'.format(cfg['SYNC_SETTINGS'].get('db'),t) for t in cfg['SYNC_SETTINGS'].get('table').split(',')]
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

def main():
   # read config
   args = parse_param()

   # parse config to dict
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
   full_sync(config)

   # diff sync
   diff_sync(config)

   # incr sync
   incr_sync(config)


if __name__ == "__main__":
     main()
