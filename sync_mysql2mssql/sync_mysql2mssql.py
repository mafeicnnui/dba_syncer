#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : ma.fei
# @File : sync_mysql2mongo.py
# @Func : mysql->mysql_syncer
# @Software: PyCharm

import sys,time
import traceback
import configparser
import warnings
import pymysql
import pymssql
import datetime
import hashlib

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_now():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_db_mysql(config):
    return pymysql.connect(host        = config['db_mysql_ip'],
                           port        = int(config['db_mysql_port']),
                           user        = config['db_mysql_user'],
                           passwd      = config['db_mysql_pass'],
                           db          = config['db_mysql_service'],
                           charset     = 'utf8',
                           cursorclass = pymysql.cursors.DictCursor)


def get_db_sqlserver(config):
    conn = pymssql.connect(host        = config['db_mssql_ip'],
                           port        = int(config['db_mssql_port']),
                           user        = config['db_mssql_user'],
                           password    = config['db_mssql_pass'],
                           database    = config['db_mssql_service'],
                           charset='utf8'
                           )
    return conn

def get_sync_time_type_name(sync_time_type):
    if sync_time_type=="day":
       return '天'
    elif sync_time_type=="hour":
       return '小时'
    elif sync_time_type=="min":
       return '分'
    else:
       return ''

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    sync_server_sour                  = cfg.get("sync","sync_db_mysql")
    sync_server_dest                  = cfg.get("sync","sync_db_server")
    config['sync_table']              = cfg.get("sync", "sync_table").lower()
    config['batch_size']              = cfg.get("sync", "batch_size")
    config['batch_size_incr']         = cfg.get("sync", "batch_size_incr")
    config['sync_time_type']          = cfg.get("sync","sync_time_type")
    config['sync_time_type_name']     = get_sync_time_type_name(config['sync_time_type'])
    db_sour_ip                        = sync_server_sour.split(':')[0]
    db_sour_port                      = sync_server_sour.split(':')[1]
    db_sour_service                   = sync_server_sour.split(':')[2]
    db_sour_user                      = sync_server_sour.split(':')[3]
    db_sour_pass                      = sync_server_sour.split(':')[4]
    db_dest_ip                        = sync_server_dest.split(':')[0]
    db_dest_port                      = sync_server_dest.split(':')[1]
    db_dest_service                   = sync_server_dest.split(':')[2]
    db_dest_user                      = sync_server_dest.split(':')[3]
    db_dest_pass                      = sync_server_dest.split(':')[4]
    config['db_mysql_ip']             = db_sour_ip
    config['db_mysql_port']           = db_sour_port
    config['db_mysql_service']        = db_sour_service
    config['db_mysql_user']           = db_sour_user
    config['db_mysql_pass']           = db_sour_pass
    config['db_mssql_ip']             = db_dest_ip
    config['db_mssql_port']           = db_dest_port
    config['db_mssql_service']        = db_dest_service
    config['db_mssql_user']           = db_dest_user
    config['db_mssql_pass']           = db_dest_pass
    config['db_mysql_string']         = db_sour_ip+':'+db_sour_port+'/'+db_sour_service
    config['db_mssql_string']         = db_dest_ip+':'+db_dest_port+'/'+db_dest_service
    config['db_mysql']                = get_db_mysql(config)
    config['db_mysql2']               = get_db_mysql(config)
    config['db_mssql']                = get_db_sqlserver(config)
    config['db_mysql2']               = get_db_sqlserver(config)
    return config

def check_mysql_tab_exists(config,tab):
   db=config['db_mysql_desc']
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema=database() and table_name='{0}'""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_mysql_tab_rows(config,tab):
   db=config['db_mysql_desc3']
   cr=db.cursor()
   sql="""select count(0) from {0}""".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_mysql_tab_incr_rows(config,tab):
   db=config['db_mysql_desc3']
   cr=db.cursor()
   sql="""select count(0) from {0}""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]


def check_mysql_tab_sync(config,tab):
   db=config['db_mysql_desc']
   cr=db.cursor()
   sql="select count(0) from {0}".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_mysql_tab_exists_pk(config,tab):
   db=config['db_mysql_sour']
   cr=db.cursor()
   sql = """select count(0) from information_schema.columns
              where table_schema=database() and table_name='{0}' and column_key='PRI'""".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_exists_pk(config,tab):
   db=config['db_sqlserver']
   cr=db.cursor()
   sql = """select
             count(0)
            from syscolumns col, sysobjects obj
            where col.id=obj.id and obj.id=object_id('{0}')
            and  (select  1
                  from  dbo.sysindexes si
                      inner join dbo.sysindexkeys sik on si.id = sik.id and si.indid = sik.indid
                      inner join dbo.syscolumns sc on sc.id = sik.id    and sc.colid = sik.colid
                      inner join dbo.sysobjects so on so.name = si.name and so.xtype = 'pk'
                  where  sc.id = col.id  and sc.colid = col.colid)=1
         """.format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_exists(config,tab):
   db=config['db_sqlserver']
   cr=db.cursor()
   sql="select count(0) from sysobjects where id=object_id(N'{0}')".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_tab_header_pkid(config,tab):
    db=config['db_mysql_sour']
    cr=db.cursor()
    cols=get_sync_table_cols_pkid(db,tab)
    sql="select {0} from {1} limit 1".format(cols,tab)
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+tab.lower()+"("
    s2=" values "
    s1=s1+cols+")"
    cr.close()
    return s1+s2

def get_tab_header(config,tab):
    db=config['db_mysql_sour']
    cr=db.cursor()
    sql="select * from {0} limit 1".format(tab)
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+tab.lower()+"("
    s2=" values "
    s1=s1+get_sync_table_cols(db,tab)+")"
    cr.close()
    return s1+s2

def f_get_table_ddl(config,tab):
    db_source = config['db_mysql_sour']
    cr_source = db_source.cursor()
    v_sql     = """SELECT   column_name,
                            data_type,
                            character_maximum_length,
                            is_nullable,
                            numeric_precision,
                            numeric_scale,
                            column_key,
                            column_default
                     FROM information_schema.columns a
                     WHERE a.table_schema=database()
                       AND a.table_name='{}'
                      ORDER BY ordinal_position
                  """.format(tab)
    cr_source.execute(v_sql)
    rs=cr_source.fetchall()
    tmp='create table {0} ('.format(tab)
    for r in rs:
       tmp = tmp +'{}   {}\n'.format(r['column_name'],r['data_type'])
    return tmp

def sync_mssql_ddl(config):
    db_source = config['db_mysql']
    cr_source = db_source.cursor()
    db_desc   = config['db_mssql']
    cr_desc   = db_desc.cursor()
    for i in config['sync_table'].split(","):
        tab=i.split(':')[0]
        if check_sqlserver_tab_exists_pk(config,tab)==0:
           print("DB:{0},Table:{1} not exist primary,ignore!".format(config['db_mysql_string'],tab))
           v_cre_sql = f_get_table_ddl(config, tab)
           if check_sqlserver_tab_exists(config, tab) > 0:
               print("DB:{0},Table :{1} already exists!".format(config['db_mysql_string'],tab))
           else:
               cr_desc.execute(v_cre_sql)
               print("Table:{0} creating success!".format(tab))
               v_pk_sql="""ALTER TABLE {0} ADD COLUMN pkid INT(11) NOT NULL AUTO_INCREMENT FIRST, ADD PRIMARY KEY (pkid)
                        """.format(tab)
               print( "Table:{0} add primary key pkid success!".format(tab))
               cr_desc.execute(v_pk_sql)
               db_desc.commit()
        else:
           #编写函数完成生成创表语句
           v_cre_sql = f_get_table_ddl(config,tab)
           if check_mysql_tab_exists(config,tab)>0:
               print("DB:{0},Table :{1} already exists!".format(config['db_mssql_string'],tab))
           else:
              cr_desc.execute(v_cre_sql)
              print("Table:{0} creating success!".format(tab))
              db_desc.commit()
    cr_source.close()
    cr_desc.close()

def get_sync_table_total_rows(db,tab,v_where):
    cr_source = db.cursor()
    v_sql="select count(0) from {0} {1}".format(tab,v_where)
    cr_source.execute(v_sql)
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sync_table_pk_names(db,tab):
    cr_source = db.cursor()
    v_col=''
    v_sql="""select column_name 
              from information_schema.columns
              where table_schema=database() 
                and table_name='{0}' and column_key='PRI' order by ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+i[0]+','
    cr_source.close()
    return v_col[0:-1]

def get_sync_table_cols(db,tab):
    cr_source = db.cursor()
    v_col=''
    v_sql="""select column_name from information_schema.columns
              where table_schema=database() and table_name='{0}'  order by ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+i[0]+','
    cr_source.close()
    return v_col[0:-1]

#获取非主键列以外的列名列表
def get_sync_table_cols_pkid(db,tab):
    return ','.join(get_sync_table_cols(db, tab).split(',')[1:])

def get_sync_table_pk_vals(db,tab):
    cr_source  = db.cursor()
    v_col=''
    v_sql="""SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + "CAST(" + i[0] + " as char)," + "\'^^^\'" + ","
    cr_source.close()
    return 'CONCAT('+v_col[0:-7]+')'

def get_sync_table_pk_vals2(db,tab):
    cr_source  = db.cursor()
    v_col=''
    v_sql="""SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col +  i[0] + ","
    cr_source.close()
    return v_col[0:-1]


def get_sync_where(pk_cols,pk_vals):
    v_where=''
    for i in range(len(pk_cols.split(','))):
        v_where=v_where+pk_cols.split(',')[i]+"='"+pk_vals.split('^^^')[i]+"' and "
    return v_where[0:-4]

def get_sync_incr_max_rq(db,tab):
    cr_desc = db.cursor()
    v_tab   = tab.split(':')[0]
    v_rq    = tab.split(':')[1]
    if v_rq=='':
       return ''
    v_sql   = "select max({0}) from {1}".format(v_rq,v_tab)
    cr_desc.execute(v_sql)
    rs=cr_desc.fetchone()
    db.commit()
    cr_desc.close()
    return rs[0]

def get_sync_where_incr_mysql(tab,config):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type']=='day':
       v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} DAY)".format(v_rq_col,v_expire_time)
    elif config['sync_time_type']=='hour':
       v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} HOUR)".format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'min':
       v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} MINUTE)".format(v_rq_col, v_expire_time)
    else:
       v = ''
    if tab.split(':')[1]=='':
       return ''
    else:
       return v

def get_sync_where_incr_mysql_rq(tab,config,currq):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} DAY)".format(v_rq_col,currq, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} HOUR)".format(v_rq_col,currq, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} MINUTE)".format(v_rq_col,currq, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1] == '':
        return ''
    else:
        return v

def get_md5(str):
    hash = hashlib.md5()
    hash.update(str.encode('utf-8'))
    return (hash.hexdigest())

def get_pk_vals_mysql(db,ftab,v_where):
    cr        = db.cursor()
    tab       = ftab.split(':')[0]
    v_pk_cols = get_sync_table_pk_vals(db, tab)
    v_sql     = "select {0} from {1} {2}".format(v_pk_cols, tab,v_where)
    cr.execute(v_sql)
    rs        = cr.fetchall()
    l_pk_vals = []
    for i in list(rs):
        l_pk_vals.append(i[0])
    cr.close()
    return l_pk_vals

def get_pk_vals_mysql2(db,ftab,v_where):
    cr        = db.cursor()
    tab       = ftab.split(':')[0]
    v_pk_cols = get_sync_table_pk_vals(db, tab)
    v_sql     = "select {0} from {1} {2}".format(v_pk_cols, tab,v_where)
    cr.execute(v_sql)
    rs        = cr.fetchall()
    v_pk_vals = ''
    for i in list(rs):
        v_pk_vals=v_pk_vals+"'"+i[0]+"',"
    cr.close()
    return v_pk_vals[0:-1]

def get_pk_vals_mysql3(rs):
    v_pk_vals=''
    for i in list(rs):
        v_pk_vals=v_pk_vals+"'"+i[0]+"',"
    return v_pk_vals[0:-1]


def sync_mysql_init(config,debug):
    config_init={}
    for i in config['sync_table'].split(","):
        tab=i.split(':')[0]
        config_init[tab] = False
        if (check_mysql_tab_exists(config,tab)==0 \
                or (check_mysql_tab_exists(config,tab)>0 and check_mysql_tab_sync(config,tab)==0)):
            #write init dict
            config_init[tab]=True

            #start first sync data
            i_counter        = 0
            ins_sql_header   = get_tab_header(config,tab)
            n_batch_size     = int(config['batch_size'])
            db_source        = config['db_mysql_sour']
            cr_source        = db_source.cursor()
            db_desc          = config['db_mysql_desc']
            cr_desc          = db_desc.cursor()

            print('delete table:{0} all data!'.format(tab))
            cr_desc.execute('delete from {0}'.format(tab))
            print('delete table:{0} all data ok!'.format(tab))

            n_tab_total_rows = get_sync_table_total_rows(db_source, tab, '')
            v_sql            = "select * from {0}".format(tab)
            cr_source.execute(v_sql)
            rs_source = cr_source.fetchmany(n_batch_size)
            while rs_source:
                batch_sql =""
                v_sql = ''
                for i in range(len(rs_source)):
                    rs_source_desc = cr_source.description
                    ins_val = ""
                    for j in range(len(rs_source[i])):
                        col_type = str(rs_source_desc[j][1])
                        if  rs_source[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == '253':  # varchar,date
                            ins_val = ins_val + "'"+ format_sql(str(rs_source[i][j])) + "',"
                        elif col_type in ('1', '3', '8', '246'):  # int,decimal
                            ins_val = ins_val + "'"+ str(rs_source[i][j]) + "',"
                        elif col_type == '12':  # datetime
                            ins_val = ins_val + "'"+ str(rs_source[i][j]).split('.')[0] + "',"
                        else:
                            ins_val = ins_val + "'"+ format_sql(str(rs_source[i][j])) + "',"
                    v_sql = v_sql +'('+ins_val[0:-1]+'),'
                batch_sql = ins_sql_header + v_sql[0:-1]
                #noinspection PyBroadException
                try:
                  cr_desc.execute(batch_sql)
                  i_counter = i_counter +len(rs_source)
                except:
                  print(traceback.format_exc())
                  print(batch_sql)
                  sys.exit(0)
                print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".format(tab,n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100,2)), end='')
                if n_tab_total_rows == 0:
                    print(
                              "Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%".format(tab, n_tab_total_rows,
                                                                                             i_counter,
                                                                                             round(i_counter / 1 * 100,
                                                                                                   2)), False)
                else:
                    print(
                              "Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%".format(tab, n_tab_total_rows,
                                                                                             i_counter, round(
                                      i_counter / n_tab_total_rows * 100, 2)), False)

                rs_source = cr_source.fetchmany(n_batch_size)
            db_desc.commit()
            print('')
    return config_init

def sync_mysql_data(config,config_init):
    db=config['db_mysql_sour']
    for v in config['sync_table'].split(","):
        tab=v.split(':')[0]
        if get_sync_table_pk_names(db,tab)=='pkid':
           sync_mysql_data_pkid(config, v,config_init)
        else:
           sync_mysql_data_no_pkid(config,v,config_init)

def sync_mysql_data_no_pkid(config, ftab,config_init):
    #start sync dml data
    if not check_full_sync(config, ftab.split(':')[0],config_init):
        i_counter        = 0
        i_counter_upd    = 0
        tab              = ftab.split(':')[0]
        db_source3       = config['db_mysql_sour3']
        db_desc3         = config['db_mysql_desc3']
        v_maxrq          = get_sync_incr_max_rq(db_desc3, ftab)
        v_where          = get_sync_where_incr_mysql(ftab, config)
        ins_sql_header   = get_tab_header(config, tab)
        n_batch_size     = int(config['batch_size_incr'])
        db_source        = config['db_mysql_sour']
        db_source2       = config['db_mysql_sour2']
        cr_source        = db_source.cursor()
        cr_source2       = db_source2.cursor()
        db_desc          = config['db_mysql_desc']
        v_pk_names       = get_sync_table_pk_names(db_source, tab)
        v_pk_cols        = get_sync_table_pk_vals(db_source, tab)
        cr_desc          = db_desc.cursor()
        n_tab_total_rows = get_sync_table_total_rows(db_source, tab, v_where)
        v_sql            = """select {0} as 'pk',{1} from {2} {3}""".format(v_pk_cols, get_sync_table_cols(db_source, tab), tab, v_where)
        #print(v_sql)
        n_rows           = 0
        cr_source.execute(v_sql)
        rs_source  = cr_source.fetchmany(n_batch_size)
        start_time = datetime.datetime.now()

        if ftab.split(':')[1] == '':
            print( "Sync Table increment :{0} ...".format(ftab.split(':')[0]))
        else:
            print( "Sync Table increment :{0} for In recent {1} {2}..."
                              .format(ftab.split(':')[0],ftab.split(':')[2],config['sync_time_type']))

        if ftab.split(':')[1] == '':
           print('DB:{0},delete {1} table data please wait...'.format(config['db_mysql_desc_string'], tab,ftab.split(':')[2]))
           cr_desc.execute('delete from {0}'.format(tab))
           print('DB:{0},delete {1} table data ok!'.format(config['db_mysql_desc_string'], tab))

        while rs_source:
            batch_sql = ""
            batch_sql_del=""
            v_sql = ''
            v_sql_del = ''
            for r in list(rs_source):
                rs_source_desc = cr_source.description
                ins_val = ""
                for j in range(1,len(r)):
                    col_type = str(rs_source_desc[j][1])
                    if r[j] is None:
                        ins_val = ins_val + "null,"
                    elif col_type == '253':  # varchar,date
                        ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                    elif col_type in ('1', '3', '8', '246'):  # int,decimal
                        ins_val = ins_val + "'" + str(r[j]) + "',"
                    elif col_type == '12':  # datetime
                        ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                    else:
                        ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                v_sql = v_sql + '(' + ins_val[0:-1] + '),'
                v_sql_del = v_sql_del + get_sync_where(v_pk_names, r[0])+ ","
            batch_sql = ins_sql_header + v_sql[0:-1]

            #noinspection PyBroadException
            try:
                if ftab.split(':')[1] == '':
                    cr_desc.execute(batch_sql)
                else:
                    for d in v_sql_del[0:-1].split(','):
                        #print('delete from {0} where {1}'.format(tab, d))
                        cr_desc.execute('delete from {0} where {1}'.format(tab, d))
                    #print(batch_sql)
                    cr_desc.execute(batch_sql)
                i_counter = i_counter + len(rs_source)
            except:
                print(traceback.format_exc())
                print(batch_sql)
                sys.exit(0)

            print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                  .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')
            if n_tab_total_rows == 0:
                print( "Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                          .format(tab, n_tab_total_rows, i_counter, round(i_counter / 1 * 100, 2)), False)
            else:
                print( "Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                          .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)),
                          False)
            rs_source = cr_source.fetchmany(n_batch_size)
        print('')
        db_desc.commit()

def sync_mysql_data_pkid(config, ftab,config_init):
    #start sync dml data
    if not check_full_sync(config, ftab.split(':')[0],config_init):
        i_counter        = 0
        tab              = ftab.split(':')[0]
        db_source3       = config['db_mysql_sour3']
        db_desc3         = config['db_mysql_desc3']
        v_maxrq          = get_sync_incr_max_rq(db_desc3, ftab)
        v_where          = get_sync_where_incr_mysql(ftab, config)
        ins_sql_header   = get_tab_header_pkid(config, tab)
        ins_sql_cols     = get_sync_table_cols_pkid(db_source3,tab)
        n_batch_size     = int(config['batch_size_incr'])
        db_source        = config['db_mysql_sour']
        db_source2       = config['db_mysql_sour2']
        cr_source        = db_source.cursor()
        cr_source2       = db_source2.cursor()
        db_desc          = config['db_mysql_desc']
        v_pk_names       = get_sync_table_pk_names(db_source, tab)
        v_pk_cols        = get_sync_table_pk_vals(db_source, tab)
        cr_desc          = db_desc.cursor()
        n_tab_total_rows = get_sync_table_total_rows(db_source, tab, v_where)
        v_sql            = """select {0} from {1} {2}""".format(ins_sql_cols,tab, v_where)
        n_rows           = 0
        cr_source.execute(v_sql)
        rs_source  = cr_source.fetchmany(n_batch_size)
        start_time = datetime.datetime.now()
        if ftab.split(':')[1] == '':
            print( "Sync Table increment :{0} ...".format(ftab.split(':')[0]))
        else:
            print( "Sync Table increment :{0} for In recent {1} {2}..."
                              .format(ftab.split(':')[0],ftab.split(':')[2],config['sync_time_type']))

        cr_desc.execute('delete from {0} {1} '.format(tab, v_where))
        print('DB:{0},delete {1} table recent {2} data...'.format(config['db_mysql_desc_string'],tab,ftab.split(':')[2]))

        while rs_source:
            batch_sql = ""
            v_sql = ''
            for i in range(len(rs_source)):
                rs_source_desc = cr_source.description
                ins_val = ""
                for j in range(len(rs_source[i])):
                    col_type = str(rs_source_desc[j][1])
                    if rs_source[i][j] is None:
                        ins_val = ins_val + "null,"
                    elif col_type == '253':  # varchar,date
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
                i_counter = i_counter + len(rs_source)
            except:
                print(traceback.format_exc())
                print(batch_sql)
                sys.exit(0)
            print("""\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%"""
                  .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')
            if n_tab_total_rows == 0:
                print( """Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%"""
                          .format(tab, n_tab_total_rows, i_counter, round(i_counter / 1 * 100, 2)), False)
            else:
                print( """Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%"""
                          .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)),
                          False)
            rs_source = cr_source.fetchmany(n_batch_size)
        db_desc.commit()
        print('')

def sync_mysql_full_data(config,debug):
    db_desc         = config['db_mysql']
    cr_desc         = db_desc.cursor()
    start_mail_time = datetime.datetime.now()
    for i in config['sync_table'].split(","):
        tab = i.split(':')[0]
        if check_full_sync(config, tab):
           sql='truncate table {0}'.format(tab)
           cr_desc.execute(sql)
           print(sql+'...ok')
    sync_mysql_init(config,debug)
    print('{0}...full sync complete'.format(tab))
    db_desc.commit()
    cr_desc.close()

def check_full_sync(config,tab,config_init):
    #if check_mysql_tab_exists_pk(config, tab) == 0 or config_init[tab]:
    if check_mysql_tab_exists_pk(config,tab)==0 :
       return True
    else:
       return False

def check_full_sync_finish(config):
    try:
        if config['full_sync_' + get_date()]:
            return True
        else:
            return False
    except:
        return False


def init(config):
    config = get_config(config)
    print_dict(config)
    return config

def sync(config):

    #sync table ddl
    sync_mssql_ddl(config)

    # #init sync table
    # config_init=sync_mysql_init(config, debug)
    #
    # #sync increment data
    # sync_mysql_data(config,config_init)


def main():
    #init variable
    config = ""
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]

    #初始化
    config=init(config)

    #数据同步
    sync(config)

if __name__ == "__main__":
     main()
