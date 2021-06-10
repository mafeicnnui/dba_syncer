#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/11/9 9:55
# @Author : 马飞
# @File : sync_mysql2rabbitmq.py.py
# @Software: PyCharm

import pymysql
import pika
import json
import datetime

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_ds_mysql(cfg):
    ip       = cfg['mysql_db'].split(':')[0]
    port     = cfg['mysql_db'].split(':')[1]
    service  = cfg['mysql_db'].split(':')[2]
    user     = cfg['mysql_db'].split(':')[3]
    password = cfg['mysql_db'].split(':')[4]
    conn     = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                               charset='utf8',cursorclass = pymysql.cursors.DictCursor)
    print(conn)
    return conn

def get_config():
    with open('./sync_mysql2rabbitmq.json', 'r') as f:
        cfg = f.read()
    cfg = json.loads(cfg)
    cfg['db'] = get_ds_mysql(cfg)
    print('cfg=', cfg)
    return cfg

def sync_mysql2rabbitmq(ch,cfg,tab):
    cr  = cfg['db'].cursor()
    st  = "select * from {0}".format(tab)
    cr.execute(st)
    rs_source = cr.fetchall()
    for r in rs_source:
        message =json.dumps(r, cls=DateEncoder,ensure_ascii=False, indent=4, separators=(',', ':'))
        print(message)
        ch.basic_publish(exchange = '',routing_key = cfg['queue_name'],body = message)

def test():
    x = "{\n    \"id\":\"0\",\n    \"name\":\"\u6839\u8282\u70b9\",\n    \"parent_id\":\"\",\n    \"url\":\"\",\n    \"status\":\"1\",\n    \"icon\":null,\n    \"creation_date\":\"2018-06-30\",\n    \"creator\":\"DBA\",\n    \"last_update_date\":\"2018-06-30\",\n    \"updator\":\"DBA\"\n}"
    y = json.loads(x)
    print(y)
    for key in y:
        if y[key] is None:
            print('{}='.format(key))
        else:
            print('{}={}'.format(key, y[key]))

def main():
    cfg  = get_config()
    tab  = cfg['sync_table']
    auth = pika.PlainCredentials(cfg['user'], cfg['password'])
    connection = pika.BlockingConnection(pika.ConnectionParameters(cfg['host'], int(cfg['port']), '/', auth))
    channel = connection.channel()
    result  = channel.queue_declare(queue=cfg['queue_name'])
    sync_mysql2rabbitmq(channel,cfg, tab)
    connection.close()


if __name__ == "__main__":
     main()


