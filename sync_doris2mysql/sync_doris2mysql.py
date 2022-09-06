#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/9/2 17:05
# @Author : ma.fei
# @File : sync_doris2mysql.py.py
# @Software: PyCharm

import os
import sys
import json
import time
import datetime
import logging
import pymysql
import traceback
import argparse

def get_ds_doris(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_ds_doris_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=False)
    return conn

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_config(cfg):
    config = cfg
    config['db_doris'] = get_ds_doris(cfg['DORIS_SETTINGS']['host'],
                                      cfg['DORIS_SETTINGS']['port'],
                                      cfg['DORIS_SETTINGS']['schema'],
                                      cfg['DORIS_SETTINGS']['user'],
                                      cfg['DORIS_SETTINGS']['passwd'])

    config['db_doris_dict'] = get_ds_doris_dict(cfg['DORIS_SETTINGS']['host'],
                                      cfg['DORIS_SETTINGS']['port'],
                                      cfg['DORIS_SETTINGS']['schema'],
                                      cfg['DORIS_SETTINGS']['user'],
                                      cfg['DORIS_SETTINGS']['passwd'])

    config['db_mysql'] = get_ds_mysql(cfg['TDSQL_SETTINGS']['host'],
                                      cfg['TDSQL_SETTINGS']['port'],
                                      cfg['TDSQL_SETTINGS']['schema'],
                                      cfg['TDSQL_SETTINGS']['user'],
                                      cfg['TDSQL_SETTINGS']['passwd'])
    config['db_doris_string'] = cfg['DORIS_SETTINGS']['host'] + ':' + cfg['DORIS_SETTINGS']['port'] + '/' + cfg['DORIS_SETTINGS']['schema']
    config['db_mysql_string'] = cfg['TDSQL_SETTINGS']['host'] + ':' + cfg['TDSQL_SETTINGS']['port'] + '/' + cfg['TDSQL_SETTINGS']['schema']
    return config

def check_mysql_tab_exists(cfg,tab):
   db=cfg['db_mysql']
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema=database() and table_name='{0}'""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_mysql_tab_data(config,tab):
   db=config['db_mysql']
   cr=db.cursor()
   sql="select count(0) from {0}".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_sync_table_cols(db,tab):
    cr = db.cursor()
    v_col=''
    v_sql="""select column_name from information_schema.columns
              where table_schema=database() and table_name='{0}'  order by ordinal_position
          """.format(tab)
    cr.execute(v_sql)
    rs_source = cr.fetchall()
    for i in list(rs_source):
        v_col=v_col+i[0]+','
    cr.close()
    return v_col[0:-1]

def f_get_table_ddl(cfg,tab):
    db = cfg['db_doris']
    cr = db.cursor()
    st ="""SELECT column_name,
                   CASE WHEN column_type='string' THEN 
                       'varchar(200)' 
                   ELSE 
                       column_type 
                   END AS column_type
                 FROM information_schema.columns
                   WHERE table_schema=DATABASE()
                     AND table_name='{}' 
                       ORDER BY ordinal_position""".format(tab)
    cr.execute(st)
    rs=cr.fetchall()
    v_cre_tab = 'create table `' + tab + '` ('
    for i in range(len(rs)):
        v_name = rs[i][0]
        v_type = rs[i][1]
        v_cre_tab = v_cre_tab + '   `' + v_name + '`    ' + v_type + ',\n'
    return v_cre_tab +' PRIMARY KEY(id))'

def get_sync_table_total_rows(db,tab,v_where):
    cr = db.cursor()
    v_sql="select count(0) from {0} {1}".format(tab,v_where)
    cr.execute(v_sql)
    rs=cr.fetchone()
    cr.close()
    return  rs[0]

def get_incr_where(tab,cfg):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if cfg['SYNC_SETTINGS']['sync_time_type']=='day':
       v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} DAY)".format(v_rq_col,v_expire_time)
    elif cfg['SYNC_SETTINGS']['sync_time_type']=='hour':
       v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} HOUR)".format(v_rq_col, v_expire_time)
    elif cfg['SYNC_SETTINGS']['sync_time_type'] == 'min':
       v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} MINUTE)".format(v_rq_col, v_expire_time)
    else:
       v = ''
    if tab.split(':')[1]=='':
       return ''
    else:
       return v

def get_sync_table_pk_names(db,tab):
    cr = db.cursor()
    v_col=''
    v_sql="""desc {}""".format(tab)
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in list(rs):
        if i['key'] == True:
           v_col=v_col+'`'+i['field']+'`,'
    cr.close()
    return v_col[0:-1]

def get_sync_table_pk_vals(db,tab):
    v_col=''
    for i in get_sync_table_pk_names(db,tab).split(','):
        v_col = v_col + "CAST(" + i[0] + " as char)," + "\'^^^\'" + ","
    return 'CONCAT('+v_col[0:-7]+')'


def get_tab_header(cfg,tab):
    db=cfg['db_doris']
    cr=db.cursor()
    sql="select * from {0} limit 1".format(tab)
    cr.execute(sql)
    s1="insert into "+tab.lower()+"("
    s2=" values "
    s1=s1+get_sync_table_cols(db,tab)+")"
    cr.close()
    return s1+s2

def ddl_sync(cfg):
    db_doris = cfg['db_doris']
    cr_doris = db_doris.cursor()
    db_mysql = cfg['db_mysql']
    cr_mysql = db_mysql.cursor()
    for obj in cfg['DORIS_SETTINGS']['table'].split(","):
        tab = obj.split(':')[0]
        cr_doris.execute("""SELECT table_schema,table_name 
                              FROM information_schema.tables
                               WHERE table_schema=database() and table_name='{0}' order by table_name
                            """.format(tab))
        rs_source = cr_doris.fetchall()
        for j in range(len(rs_source)):
            if check_mysql_tab_exists(cfg, tab) > 0:
                print("TDSQL:{0},Table :{1} already exists!".format(cfg['db_doris_string'], tab))
            else:
                ddl = f_get_table_ddl(cfg, tab)
                if cfg['SYNC_SETTINGS']['debug'] == 'Y':
                   print(ddl)
                cr_mysql.execute(ddl)
                print("TDSQL:{0},Table:{1} creating success!".format(cfg['db_doris_string'],tab))
                db_mysql.commit()
    cr_doris.close()
    cr_mysql.close()

def full_sync(cfg):
    for obj in cfg['DORIS_SETTINGS']['table'].split(","):
        tab = obj.split(':')[0]
        if (check_mysql_tab_exists(cfg, tab) == 0
                or (check_mysql_tab_exists(cfg, tab) > 0 and check_mysql_tab_data(cfg, tab) == 0)):
            i_counter = 0
            ins_sql_header = get_tab_header(cfg, tab)
            n_batch_size = int(cfg['SYNC_SETTINGS']['batch_size'])
            db_doris = cfg['db_doris']
            cr_source = db_doris.cursor()
            db_mysql = cfg['db_mysql']
            cr_desc = db_mysql.cursor()

            n_tab_total_rows = get_sync_table_total_rows(db_doris, tab, '')
            v_sql = "select * from {0}".format(tab)
            cr_source.execute(v_sql)
            rs_source = cr_source.fetchmany(n_batch_size)
            while rs_source:
                v_sql = ''
                for i in range(len(rs_source)):
                    rs_source_desc = cr_source.description
                    ins_val = ""
                    for j in range(len(rs_source[i])):
                        col_type = str(rs_source_desc[j][1])
                        if rs_source[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type in('253','252'):  # varchar,date
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                        elif col_type in ('1', '3', '8', '246'):  # int,decimal
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        elif col_type == '12':  # datetime
                            ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                        else:
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                    v_sql = v_sql + '(' + ins_val[0:-1] + '),'
                batch_sql = ins_sql_header + v_sql[0:-1]

                # noinspection PyBroadException
                try:
                    cr_desc.execute(batch_sql)
                    db_mysql.commit()
                    i_counter = i_counter + len(rs_source)
                except:
                    print(traceback.format_exc())
                    sys.exit(0)
                print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                      format(tab,
                             n_tab_total_rows,
                             i_counter,
                             round(i_counter / n_tab_total_rows * 100,2)), end='')
                rs_source = cr_source.fetchmany(n_batch_size)
            print('')

def incr_sync(cfg):
    for obj in cfg['DORIS_SETTINGS']['table'].split(","):
        tab = obj.split(':')[0]
        i_counter = 0
        db_doris = cfg['db_doris']
        db_doris_dict = cfg['db_doris_dict']
        cr_source = db_doris.cursor()
        db_mysql = cfg['db_mysql']
        cr_desc = db_mysql.cursor()

        v_pk_names = get_sync_table_pk_names(db_doris_dict, tab)
        v_pk_cols = get_sync_table_pk_vals(db_doris_dict, tab)

        print('v_pk_names=',v_pk_names)
        print('v_pk_cols=',v_pk_cols)


        v_where = get_incr_where(obj, cfg)
        ins_sql_header = get_tab_header(cfg, tab)
        n_batch_size = int(cfg['SYNC_SETTINGS']['batch_size_incr'])
        n_tab_total_rows = get_sync_table_total_rows(db_doris, tab, v_where)
        v_sql = """select * from {} {}""".format(tab,v_where)
        # print(v_sql)
        n_rows = 0
        cr_source.execute(v_sql)
        rs_source = cr_source.fetchmany(n_batch_size)
        start_time = datetime.datetime.now()

        if obj.split(':')[1] == '':
            print('DB:{0},delete {1} table data please wait...'.format(cfg['db_mysql_string'], tab))
            cr_desc.execute('delete from {0}'.format(tab))
            print('DB:{0},delete {1} table data ok!'.format(cfg['db_mysql_string'], tab))

        while rs_source:
            batch_sql = ""
            batch_sql_del = ""
            v_sql = ''
            v_sql_del = ''
            for r in list(rs_source):
                rs_source_desc = cr_source.description
                ins_val = ""
                for j in range(1, len(r)):
                    col_type = str(rs_source_desc[j][1])
                    if r[j] is None:
                        ins_val = ins_val + "null,"
                    elif col_type in('252','253') :  # varchar,date
                        ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                    elif col_type in ('1', '3', '8', '246'):  # int,decimal
                        ins_val = ins_val + "'" + str(r[j]) + "',"
                    elif col_type == '12':  # datetime
                        ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                    else:
                        ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                v_sql = v_sql + '(' + ins_val[0:-1] + '),'
                v_sql_del = v_sql_del + get_sync_where(v_pk_names, r[0]) + ","
            batch_sql = ins_sql_header + v_sql[0:-1]

            # noinspection PyBroadException
            try:
                if ftab.split(':')[1] == '':
                    cr_desc.execute(batch_sql)
                else:
                    for d in v_sql_del[0:-1].split(','):
                        # print('delete from {0} where {1}'.format(tab, d))
                        cr_desc.execute('delete from {0} where {1}'.format(tab, d))
                    # print(batch_sql)
                    cr_desc.execute(batch_sql)
                i_counter = i_counter + len(rs_source)
            except:
                print(traceback.format_exc())
                sys.exit(0)

            print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                  .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')

            rs_source = cr_source.fetchmany(n_batch_size)
        print('')
        db_desc.commit()


'''
   1.table must have `id` column.
   
'''
def main():
   # read config
   args = parse_param()

   # parse config to dict
   cfg = read_json(args.conf)

      # init logger
   logging.basicConfig(
       filename=cfg['SYNC_SETTINGS']['logfile'],
       format='[%(asctime)s-%(levelname)s:%(message)s]',
       level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

   # init config
   config = get_config(cfg)

   # print config
   print_dict(config)

   # sync table ddl
   ddl_sync(config)

   # full sync
   full_sync(config)

   # incr sync
   #incr_sync(config)


if __name__ == "__main__":
     main()
