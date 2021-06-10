#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm
# @func:通过urllib库以以节字泫发送(https协议)post请求

import sys,time
import traceback
import configparser
import warnings
import pymysql
import datetime
import smtplib
import json
from email.mime.text import MIMEText
import urllib.parse
import urllib.request
import ssl
#ssl._create_default_https_context = ssl._create_unverified_context

def send_mail465(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
        return 0
    except smtplib.SMTPException as e:
        print(e)
        return -1

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_now():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_db_mysql(config):
    return get_ds_mysql(config['db_mysql_ip'],config['db_mysql_port'],config['db_mysql_service'],\
                        config['db_mysql_user'],config['db_mysql_pass'])

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    db_mysql                          = cfg.get("sync","db_mysql")
    config['db_mysql_ip']             = db_mysql.split(':')[0]
    config['db_mysql_port']           = db_mysql.split(':')[1]
    config['db_mysql_service']        = db_mysql.split(':')[2]
    config['db_mysql_user']           = db_mysql.split(':')[3]
    config['db_mysql_pass']           = db_mysql.split(':')[4]
    config['send_gap']                = cfg.get("sync", "send_gap")
    config['send_user']               = cfg.get("sync", "send_mail_user")
    config['send_pass']               = cfg.get("sync", "send_mail_pass")
    config['acpt_user']               = cfg.get("sync", "acpt_mail_user")
    config['mail_title']              = cfg.get("sync", "mail_title")
    config['hopson_interface']        = cfg.get("sync","hopson_interface")
    config['db_mysql_string']         = config['db_mysql_ip'] +':'+config['db_mysql_port'] +'/'+config['db_mysql_service']
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
   sql="""select count(0) from {0}""".format(tab )
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

def init(config):
    config = get_config(config)
    #print dict
    print_dict(config)
    return config

#判断待办任务是否推送过消息
def isSend(config,id):
    db = get_db_mysql(config)
    cr = db.cursor()
    sql = "select count(0) from ot_workitem_ext where id={0} and  isSend='Y'".format(id)
    cr.execute(sql)
    rs=cr.fetchone()
    if rs[0]>0:
       return True
    else:
       return False

def get_relationId(config,v_tab,v_bizid):
    db = get_db_mysql(config)
    cr = db.cursor()
    sql = "select id from {0} where objectid='{1}'".format(v_tab,v_bizid)
    try:
       cr.execute(sql)
       rs = cr.fetchone()
       cr.close()
       return rs[0]
    except:
       return ''

def get_itemComment(config,objectid):
    db  = get_db_mysql(config)
    cr  = db.cursor()
    sql = "select itemComment from ot_workitemfinished where objectid='{0}'".format(objectid)
    cr.execute(sql)
    rs1 = cr.fetchone()
    if rs1 is None or rs1[0]=='':
       sql='''
              SELECT TEXT FROM ot_comment m 
              WHERE m.WorkItemId='{0}'
                and  m.modifiedtime=(select max(modifiedtime) from ot_comment 
                                     where WorkItemId='{1}') limit 1
           '''.format(objectid,objectid)
       cr.execute(sql)
       rs2 = cr.fetchone()
       cr.close()
       if rs2 is None or rs2[0] == '':
          return ''
       else:
          return rs2[0]
    else:
       cr.close()
       return rs1[0]


#处理消息类型：1.延时闭店，2.携物出门，3.营运期施工
def send_message_easylife(config,debug):
    db  = get_db_mysql(config)
    db2 = get_db_mysql(config)
    cr  = db.cursor()
    cr2 = db.cursor()

    sql= """
         SELECT 
             b.workflowCode              AS '流程模板编码',
             CONCAT('i_',b.workflowCode) AS '流程模板表',
             b.state                     AS '实例状态',
             b.bizobjectid               AS '流程模板表ID', 
             a.participantName           AS '参与者姓名',
             a.displayName               AS '活动显示名称',
             #a.ActionName                AS '操作名称',
             IF(a.ActionName='' AND a.approval!=0,'Submit',a.ActionName) AS '操作名称', 
             a.itemComment               AS '当前征询意见填写的意见',
             a.finishtime                AS '接受时间',
             e.id                        AS '扩展表主键',
             a.objectid                  as 'OBJECTID',
             a.InstanceId                as 'instanceId'
         FROM ot_workitemfinished a,
              ot_instancecontext b,
              ot_workitemfinished_ext e 
          WHERE a.InstanceId=b.objectid  
           AND e.task_id= a.objectID 
           AND a.FinishTime>'2019-07-15'
           AND b.workflowcode LIKE 'hsh_%'
           AND e.isSend='N'    
           order by a.finishtime       
        """

    cr.execute(sql)
    rs = cr.fetchall()
    for i in list(rs):
        n_relationId  = get_relationId(config, i[1],i[3])
        v_itemComment = get_itemComment(config, i[10])
        message = {
            'relationId'     : n_relationId,
            'state'          : i[2],
            'workflowCode'   : i[0],
            'participantName': i[4],
            'displayName'    : i[5],
            'actionName'     : i[6],
            'itemComment'    : v_itemComment,   #i[7]
            'receiveTime'    : str(i[8]),
            'instanceId'     : str(i[11])
        }

        v_message = json.dumps(message)
        print('v_message=',v_message)

        values = {
            'message': v_message
        }

        #调用接口推送消息
        n_failure_time = 0
        while True:
            try:
                url     = config['hopson_interface']
                context = ssl._create_unverified_context()
                data    = urllib.parse.urlencode(values).encode(encoding='UTF-8')
                print('data=',data)
                req     = urllib.request.Request(url,data=data)
                res     = urllib.request.urlopen(req,context=context)
                res     = json.loads(res.read())
                print(res,res['code'])
                if res['code'] == 200:
                    print('接口调用成功!')
                    cr.execute("update ot_workitemfinished_ext t set isSend='Y' where id={0}".format(i[9]))
                    db.commit()
                    print('扩展表状态更新成功!')
                    n_failure_time=0
                    break
                else:
                    print(res['msg'])
                    break
            except:
                print(traceback.format_exc())
                print('接口调用失败,第{0}次重试中...!'.format(str(n_failure_time+1)))
                n_failure_time = n_failure_time + 1
                time.sleep(60)
        time.sleep(5)
    cr.close()

#查询是否有加载待办任务扩展信息
def get_undone_task_ext(config):
    db=get_db_mysql(config)
    cr=db.cursor()
    sql='''select  count(0)
              FROM ot_workitemfinished t
              WHERE  t.receiveTime>='2019-07-03'
                 AND t.workflowcode LIKE 'hsh%'
                 AND NOT EXISTS(SELECT 1 FROM ot_workitemfinished_ext e
                                WHERE e.task_id= t.objectID)
        '''
    cr.execute(sql)
    rs=cr.fetchone()
    cr.close()
    return rs[0]

#加载待办任务至扩展表【合生通】
def write_undone_task_ext(config):
    db=get_db_mysql(config)
    cr=db.cursor()
    if get_undone_task_ext(config)==0:
       print('未找到新的待办任务!')
    else:
       sql='''INSERT INTO ot_workitemfinished_ext(inst_id,task_id) 
              SELECT  t.instanceid, t.objectID
              FROM ot_workitemfinished t
              WHERE  t.receiveTime>='2019-07-03'
                 AND t.workflowcode LIKE 'hsh%'
                 AND NOT EXISTS(SELECT 1 FROM ot_workitemfinished_ext e
                                WHERE e.task_id= t.objectID)
            '''
       cr.execute(sql)
       db.commit()
       print('采集到新的待办任务!')
    cr.close()

#消息推送
def push(config,debug):
    #循环临听待办任务表变化，变更新扩展表进行消息推送处理
    while True:
      #将未推送消息待办任务写入扩展表
      write_undone_task_ext(config)
      #推送合生通消息
      send_message_easylife(config,debug)
      #休眠
      print('休眠 {0}秒...'.format(config['send_gap']))
      time.sleep(int(config['send_gap']))

def main():
    #init variable
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-conf":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    #初始化
    config=init(config)

    #process
    push(config,debug)

if __name__ == "__main__":
     main()
