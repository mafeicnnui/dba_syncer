#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time : 2021/1/3 15:09
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm

import sys
import json
import datetime
import traceback
import warnings
import cx_Oracle
import pymysql

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        else:
            return json.JSONEncoder.default(self, obj)

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def format_sql(v_sql):
    return v_sql.replace("'","''")

def print_dict(config):
    print('\nPrint config information...')
    print('-'.ljust(50, '-'))
    print(json.dumps(config, cls=DateEncoder, ensure_ascii=False, indent=4, separators=(',', ':')) + '\n')

def get_ds_mysql(config):
    v_ip   = config['mysql_server']['host']
    n_port = int(config['mysql_server']['port'])
    v_db   = config['mysql_server']['database']
    v_user = config['mysql_server']['user']
    v_pass = config['mysql_server']['password']
    v_charset = config['mysql_server']['charset']
    conn = pymysql.connect(host=v_ip, port=int(n_port), user=v_user, passwd=v_pass, db=v_db, charset=v_charset)
    return conn

def get_ds_oracle(config):
    v_ip         = config['oracle_server']['host']
    n_port       = int(config['oracle_server']['port'])
    v_instance   = config['oracle_server']['instance']
    v_user       = config['oracle_server']['user']
    v_pass       = config['oracle_server']['password']
    tns          = cx_Oracle.makedsn(v_ip,n_port,v_instance)
    db           = cx_Oracle.connect(v_user,v_pass,tns)
    return db

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def get_config():
    config = read_json('sync_oracle2mysql.json')
    print_dict(config)
    config['db_oracle'] = get_ds_oracle(config)
    config['db_mysql']  = get_ds_mysql(config)
    return config

def check_oracle_tab_exists_pk(config,tab):
   db  = config['db_oracle']
   cr  = db.cursor()
   sql = """select  count(0) from user_constraints where  table_name='{}' and constraint_type='P'
         """.format(tab.upper())
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   if rs[0]>0:
      return True
   else:
      return False

def check_oracle_tab_exists(config,tab):
   db = config['db_oracle']
   cr = db.cursor()
   sql = """select  count(0) from user_tables where table_name='{}'
         """.format(tab.upper())
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   if rs[0]>0:
      return True
   else:
      return False

def get_tab_header(config,tab):
    s1="insert into "+tab.lower()+"("
    s2=" values "
    s1=s1+get_sync_table_cols(config,tab)+") "
    return s1+s2

def get_mysql_table_rows(config,tab,v_where):
    db = config['db_mysql']
    cr = db.cursor()
    v_sql="select count(0) from {0} {1}".format(tab,v_where)
    cr.execute(v_sql)
    rs=cr.fetchone()
    cr.close()
    return  rs[0]

def get_oracle_table_total_rows(config,tab):
    db = config['db_oracle']
    cr = db.cursor()
    v_sql="select count(0) from {0}".format(tab)
    cr.execute(v_sql)
    rs=cr.fetchone()
    cr.close()
    return  rs[0]

def get_sync_table_cols(config,tab):
    db = config['db_oracle']
    cr = db.cursor()
    v_col=''
    v_sql="""select b.column_name from user_tab_columns b where   b.table_name='{}'  order by b.column_id
          """.format(tab.upper())
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in list(rs):
        v_col=v_col+i[0].lower()+','
    cr.close()
    return v_col[0:-1]

def get_sync_table_pk_names(config,tab):
    db    = config['db_oracle']
    cr    = db.cursor()
    v_col = ''
    v_sql = """select b.column_name from dba_cons_columns a,dba_tab_columns b 
                where a.owner=b.owner and a.table_name=b.table_name and a.column_name=b.column_name
                  and a.constraint_name=(select constraint_name from dba_constraints b 
                          where b.owner='{}' and b.table_name='{}' and constraint_type='P') order by b.column_id
            """.format(config['oracle_server']['user'].upper(), tab.upper())
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in list(rs):
        v_col=v_col+'['+i[0]+'],'
    cr.close()
    return v_col[0:-1]

def get_sync_table_pk_vals(config,tab):
    db = config['db_oracle']
    cr = db.cursor()
    v_col=''
    v_sql = """select b.column_name from dba_cons_columns a,dba_tab_columns b 
                    where a.owner=b.owner and a.table_name=b.table_name and a.column_name=b.column_name
                      and a.constraint_name=(select constraint_name from dba_constraints b 
                              where b.owner='{}' and b.table_name='{}' and constraint_type='P') order by b.column_id
                """.format(config['oracle_server']['user'].upper(), tab.upper())
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in list(rs):
        v_col = v_col + "to_char({})||" + "\'^^^\'" + "||"
    cr.close()
    return v_col[0:-7]

def get_sync_where(pk_cols,pk_vals):
    v_where=''
    for i in range(len(pk_cols.split(','))):
        v_where=v_where+pk_cols.split(',')[i]+"='"+pk_vals.split('^^^')[i]+"' and "
    return v_where[0:-4]

def get_sync_where_incr(tab,config):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = 'where {0} >=DATEADD(DAY,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = 'where {0} >=DATEADD(HOUR,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = 'where {0} >=DATEADD(MINUTE,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1]=='':
       return ''
    else:
       return v

def check_mysql_tab_exists(config,tab):
   db = config['db_mysql']
   cr = db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema=database() and table_name='{0}'""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   if rs[0]>0:
      return True
   else:
      return False

def check_mysql_tab_exists_pk(config,tab):
   db  = config['db_mysql']
   cr  = db.cursor()
   sql = """select count(0) from information_schema.columns
              where table_schema=database() and table_name='{0}' and column_key='PRI'""".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   if rs[0] > 0:
       return True
   else:
       return False

def full_sync(config):
    config_init={}
    for i in config['sync_tables']:
        tab=i.split(':')[0]
        config_init[tab] = False
        if check_mysql_tab_exists(config, tab) and get_mysql_table_rows(config, tab,'') == 0:
            config_init[tab] = True
            i_counter        = 0
            ins_sql_header   = get_tab_header(config,tab)
            n_batch_size     = int(config['batch_size'])
            odb              = config['db_oracle']
            ocr              = odb.cursor()
            mdb              = config['db_mysql']
            mcr              = mdb.cursor()

            print('MySQL:delete table:{0} all data!'.format(tab))
            mcr.execute('delete from {0}'.format(tab))
            print('MySQL:delete table:{0} all data ok!'.format(tab))

            n_tab_total_rows = get_oracle_table_total_rows(config, tab)
            v_sql  = "select * from {0}".format(tab)
            print('Oracle:exec sql:{}'.format(v_sql))
            ocr.execute(v_sql)
            ors = ocr.fetchmany(n_batch_size)
            while ors:
                v_sql   = ''
                for i in range(len(ors)):
                    ors_desc = ocr.description
                    ins_val = ""
                    for j in range(len(ors[i])):
                        col_type = str(ors_desc[j][1])
                        if  ors[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type.count('STRING') >0 :
                            ins_val = ins_val + "'"+ format_sql(str(ors[i][j])) + "',"
                        elif col_type.count('NUMBER') >0 :  # int,decimal
                            ins_val = ins_val +  str(ors[i][j]) + ","
                        elif col_type.count('DATE')>0:  # datetime
                            ins_val = ins_val + "'"+ str(ors[i][j]).split('.')[0] + "',"
                        else:
                            ins_val = ins_val + "'"+ format_sql(str(ors[i][j])) + "',"
                    v_sql = v_sql +'('+ins_val[0:-1]+'),'
                v_batch = ins_sql_header + v_sql[0:-1]

                #noinspection PyBroadException
                try:
                  mcr.execute(v_batch)
                  i_counter = i_counter +len(ors)
                except:
                  print(traceback.format_exc())
                  print(v_batch)
                  sys.exit(0)
                print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".format(tab,n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100,2)), end='')
                if n_tab_total_rows == 0:
                    print("Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                          .format(tab, n_tab_total_rows,i_counter,round(i_counter / 1 * 100,2)), False)
                else:
                    print("Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                          .format(tab, n_tab_total_rows,i_counter, round(i_counter / n_tab_total_rows * 100, 2)), False)

                ors = ocr.fetchmany(n_batch_size)
            mdb.commit()
            print('')
    return config_init

def incr_sync(config,ftab):
    for i in config['sync_tables']:
        tab=i.split(':')[0]
        if check_oracle_tab_exists(config, tab):
            i_counter        = 0
            v_where          = get_sync_where_incr(ftab,config)
            n_tab_total_rows = get_oracle_table_total_rows(config,tab,v_where)
            ins_sql_header   = get_tab_header(config,tab)
            v_pk_names       = get_sync_table_pk_names(config, tab)
            v_pk_cols        = get_sync_table_pk_vals(config, tab)
            n_batch_size     = int(config['batch_size'])
            db_source        = config['db_sqlserver_sour']
            cr_source        = db_source.cursor()
            db_desc          = config['db_sqlserver_dest']
            cr_desc          = db_desc.cursor()
            v_sql            = """select {0} as 'pk',{1} from {2} with(nolock) {3}
                               """.format(v_pk_cols,get_sync_table_cols(config,tab), tab,v_where)
            n_rows           = 0
            cr_source.execute(v_sql)
            rs_source        = cr_source.fetchmany(n_batch_size)
            start_time       = datetime.datetime.now()

            if ftab.split(':')[1]=='':
                print("Sync Table increment :{0} ...".format(ftab.split(':')[0]))
            else:
                print("Sync Table increment :{0} for In recent {1} {2}...".
                      format(ftab.split(':')[0], ftab.split(':')[2],config['sync_time_type']))

            while rs_source:
                v_sql_tmp = ''
                v_sql_ins = ''
                v_sql_ins_batch =''
                v_sql_del_batch =''

                if check_tab_identify(tab, config) > 0:
                    v_sql_ins_batch = 'begin\nset identity_insert {0} ON\n'.format(tab)
                    v_sql_del_batch = 'begin\nset identity_insert {0} ON\n'.format(tab)
                else:
                    v_sql_ins_batch = 'begin\n'
                    v_sql_del_batch = 'begin\n'

                n_rows=n_rows+len(rs_source)
                #print("\r{0},Scanning table:{1},{2}/{3} rows,elapsed time:{4}s...".
                #      format(get_time(),tab,str(n_rows),str(n_tab_total_rows),str(get_seconds(start_time))),end='')
                rs_source_desc = cr_source.description
                if len(rs_source) > 0:
                    for r in list(rs_source):
                        v_sql_tmp = ''
                        for j in range(1, len(r)):
                            col_type = str(rs_source_desc[j][1])
                            if r[j] is None:
                                v_sql_tmp = v_sql_tmp + "null,"
                            elif col_type == "1":  # varchar,date
                                v_sql_tmp = v_sql_tmp + "'" + format_sql(str(r[j])) + "',"
                            elif col_type == "5":  # int,decimal
                                v_sql_tmp = v_sql_tmp + "'" + str(r[j]) + "',"
                            elif col_type == "4":  # datetime
                                v_sql_tmp = v_sql_tmp + "'" + str(r[j]).split('.')[0] + "',"
                            elif col_type == "3":  # bit
                                if str(r[j]) == "True":  # bit
                                    v_sql_tmp = v_sql_tmp + "'" + "1" + "',"
                                elif str(r[j]) == "False":  # bit
                                    v_sql_tmp = v_sql_tmp + "'" + "0" + "',"
                                else:  # bigint ,int
                                    v_sql_tmp = v_sql_tmp + "'" + str(r[j]) + "',"
                            elif col_type == "2":  # timestamp
                                v_sql_tmp = v_sql_tmp + "null,"
                            else:
                                v_sql_tmp = v_sql_tmp + "'" + format_sql(str(r[j])) + "',"

                        v_where   = get_sync_where(v_pk_names, r[0])
                        v_sql_ins = ins_sql_header  + '(' + v_sql_tmp[0:-1]+ ')'
                        v_sql_del = "delete from {0} where {1}".format(tab,v_where)
                        v_sql_ins_batch = v_sql_ins_batch + v_sql_ins+'\n'
                        v_sql_del_batch = v_sql_del_batch + v_sql_del + '\n'

                if check_tab_identify(tab, config) > 0:
                   v_sql_ins_batch = v_sql_ins_batch + 'set identity_insert {0} OFF\nend;'.format(tab)
                   v_sql_del_batch = v_sql_del_batch + 'set identity_insert {0} OFF\nend;'.format(tab)
                else:
                   v_sql_ins_batch = v_sql_ins_batch+'\nend;'
                   v_sql_del_batch = v_sql_del_batch+'\nend;'

                try:
                   cr_desc.execute(v_sql_del_batch)
                   cr_desc.execute(v_sql_ins_batch)
                   i_counter = i_counter + len(rs_source)
                   rs_source = cr_source.fetchmany(n_batch_size)
                except:
                   print(traceback.format_exc())
                   print('v_sql_del_batch=', v_sql_del_batch)
                   print('v_sql_ins_batch=', v_sql_ins_batch)
                   sys.exit(0)

                print("\rTable:{0},Total :{1},Process ins:{2},Complete:{3}%,elapsed time:{4}s"
                      .format(tab,
                              n_tab_total_rows,
                              i_counter,
                              round((i_counter) / n_tab_total_rows * 100, 2),
                              str(get_seconds(start_time))))

            if n_tab_total_rows == 0:
                print("Table:{0},Total :0,skip increment sync!".format(tab))
            else:
                print("\tTable:{0},Total :{1},Process ins:{2},Complete:{3}%,elapsed time:{4}s"
                      .format(tab,
                              n_tab_total_rows,
                              i_counter,
                              round((i_counter) / n_tab_total_rows * 100, 2),
                              str(get_seconds(start_time))))
            db_desc.commit()

def f_get_table_ddl(config,tab):
    db = config['db_oracle']
    cr = db.cursor()
    v_sql     ="""select 
                       column_name,
                       data_type,
                       data_length,
                       data_precision,
                       data_scale,
                       nullable,
                       (select decode(abs(count(0)),1,'Y','N')
                         from user_cons_columns 
                         where column_name=a.column_name 
                           and constraint_name=(select constraint_name 
                                                  from user_constraints 
                                                    where table_name='{}' 
                                                      and constraint_type='P')) as ISPK
                from user_tab_columns a
                where a.table_name='{}'    
                order by a.column_id """.format(tab.upper(),tab.upper())
    cr.execute(v_sql)
    rs=cr.fetchall()
    v_cre_tab= 'create table '+tab+'('
    for i in range(len(rs)):
        v_name     = rs[i][0]
        v_type     = rs[i][1]
        v_len      = str(rs[i][2])
        v_len_int  = str(rs[i][3])
        v_scale    = str(rs[i][4])
        v_null     = str(rs[i][5])
        v_identify = str(rs[i][6])

        if v_null == 'N':
           v_null =' not null '
        else:
           v_null =''

        if v_identify !='N':
           v_identify = ' primary key '
        else:
           v_identify = ''

        if v_type in ('NUMBER'):
            if v_scale == '0':
                v_type = 'bigint'
                v_cre_tab = v_cre_tab + '   `' + v_name + '`    ' + v_type + v_null + v_identify + ','
            else:
                v_type = 'numeric'
                v_cre_tab = v_cre_tab + '   `' + v_name + '`    ' + v_type + '(' + v_len_int + ',' + v_scale + ') ' + v_null + v_identify + ','
        elif v_type in('VARCHAR2'):
            v_type = 'varchar'
            v_cre_tab = v_cre_tab + '   `' + v_name + '`    ' + v_type +'('+ v_len+') '+v_null+v_identify+','
        else:
            v_cre_tab = v_cre_tab + '   `' + v_name + '`    ' + v_type + v_null + v_identify + ','

    return v_cre_tab[0:-1].lower()+')'

def ddl_sync(config):
    odb = config['db_oracle']
    ocr = odb.cursor()
    mdb   = config['db_mysql']
    mcr   = mdb.cursor()
    for i in config['sync_tables']:
        tab=i.split(':')[0]
        if not check_oracle_tab_exists(config,tab):
           print('Oracle: table {} not exists,skiped!'.format(tab))
           continue

        if not check_oracle_tab_exists_pk(config, tab):
           print("Oracle: table {1} not exist primary key!".format(config,tab))
           continue

        if not check_mysql_tab_exists(config, tab):
            v_cre_sql = f_get_table_ddl(config, tab)
            mcr.execute(v_cre_sql)
            mdb.commit()
            print('MySQL: table {} created!'.format(tab))
        else:
            print('MySQL: table {} already exists!'.format(tab))

    ocr.close()
    mcr.close()

def main():
    warnings.filterwarnings("ignore")
    config = get_config()

    print('Syncing table structure...')
    ddl_sync(config)

    print('Full sync table...')
    full_sync(config)

if __name__ == "__main__":
     main()
