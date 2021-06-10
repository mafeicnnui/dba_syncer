#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm
import sys,time
import traceback
import configparser
import warnings
import pymysql
import datetime
import smtplib
import json
from email.mime.text import MIMEText
import requests

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

def get_ysbd_info(config,v_tab,v_id):
    db = get_db_mysql(config)
    cr = db.cursor()
    sql = """select createdtime,
                    IFNULL(yskssj,'') AS yskssj,
                    IFNULL(ysjssj,'') AS ysjssj,
                    CASE WHEN ysyy='其他' THEN qtyy ELSE ysyy END AS content
             from {0} where objectid='{1}'
           """.format(v_tab,v_id)
    cr.execute(sql)
    rs=cr.fetchone()
    cr.close()
    return rs[0],rs[1],rs[2],rs[3]

def get_xwcm_info(config,v_tab,v_id):
    db = get_db_mysql(config)
    cr = db.cursor()
    sql = """SELECT createdtime,
                    IFNULL(cmkssj,'') AS cmkssj,
                    IFNULL(cmjssj,'') AS cmjssj,
                    CONCAT(wppl,'\n',wpmx) AS content
             from {0} where objectid='{1}'
           """.format(v_tab,v_id)
    cr.execute(sql)
    rs=cr.fetchone()
    cr.close()
    return rs[0],rs[1],rs[2],rs[3]


def get_yyqsg_info(config,v_tab,v_id):
    db = get_db_mysql(config)
    cr = db.cursor()
    sql = """SELECT createdtime,
                    IFNULL(sgkssj,'') AS sgkssj,
                    IFNULL(sgjssj,'') AS sgjssj,
                    sgnr AS content
             from {0} where objectid='{1}'
           """.format(v_tab,v_id)
    cr.execute(sql)
    rs=cr.fetchone()
    cr.close()
    return rs[0],rs[1],rs[2],rs[3]


#处理消息类型：1.延时闭店，2.携物出门，3.营运期施工
def send_message_hst(config,debug):
    db  = get_db_mysql(config)
    db2 = get_db_mysql(config)
    cr  = db.cursor()
    cr2 = db.cursor()
    v_workflow_type_ysbd   = 'YSBD'
    v_workflow_type_xwcm   = 'XWCM'
    v_workflow_type_yyygsg = 'YYQSG'

    sql= """SELECT 
                 u.code                       AS '参与者',
                 '2'                          AS '绑定类型',
                 oi.SequenceNo                AS '流程编码',
                 oi.InstanceName              AS '申请商户名称',
                 SUBSTR(t.workflowcode,4)     AS '流程类别',
                 CONCAT('i_',t.workflowcode)  AS '流程模板表',
                 oi.bizobjectid               AS '流程表单ID',
                 u2.code                      AS '发起人账号',
                 u2.name                      AS '发起人姓名',
                 e.id                         AS '扩展表主键'
            FROM ot_workitem t,
                 ot_workitem_ext e ,
                 ot_instancecontext oi,
                 ot_user u,
                 ot_user u2
            WHERE e.task_id= t.objectID 
                AND t.instanceid=oi.objectid
                AND u2.objectid=oi.Originator
                AND t.Participant=u.objectid              
                AND t.workflowcode REGEXP '^[1-9][0-9][0-9]'
                AND u.code REGEXP '^[1-9]'
                AND u2.code REGEXP '^[1-9]'
                AND SUBSTR(t.workflowcode,4)  IN('YSBD','XWCM','YYQSG')
                AND t.activitycode!='1'
                AND e.isSend='N'"""
    cr.execute(sql)
    rs = cr.fetchall()
    for i in list(rs):
        v_type=i[4]
        if v_type == v_workflow_type_ysbd:
            v_applicantTime,v_beginTime,v_endTime,v_content=get_ysbd_info(config,i[5],i[6])
        elif v_type == v_workflow_type_xwcm:
            v_applicantTime,v_beginTime,v_endTime,v_content=get_xwcm_info(config,i[5],i[6])
        elif v_type == v_workflow_type_yyygsg:
            v_applicantTime,v_beginTime,v_endTime,v_content=get_yyqsg_info(config,i[5],i[6])
        else:
            v_applicantTime, v_beginTime, v_endTime, v_content = '','','',''

        message = {
            'account': i[0],
            'bindingType': i[1],
            'code': i[2],
            'applicantPerson': i[3],
            'workflowtype' :i[4],
            'applicantTime': str(v_applicantTime),
            'beginTime': str(v_beginTime),
            'endTime'  : str(v_endTime),
            'content'  : v_content,
            'applicantPersonCode': i[7]
        }

        v_message = json.dumps(message)

        values = {
            'message': v_message
        }
        print('v_message=', v_message)
        print('values=', values)

        #调用接口推送消息
        n_failure_time = 0
        while True:
            try:
                url = config['hopson_interface']
                print(json.dumps(values))
                r   = requests.post(url, data=values, verify=False)
                print('r.text=',r.text)
                res = json.loads(r.text)
                print('response=', r.text,json.loads(r.text))
                if res['code'] == 200:
                    print('接口调用成功!')
                    cr.execute("update ot_workitem_ext t set isSend='Y' where id={0}".format(i[9]))
                    db.commit()
                    print('扩展表状态更新成功!')
                    n_failure_time=0
                    break
                else:
                    print('接口调用失败!')
                    cr.execute("update ot_workitem_ext t set isSend='Y' where id={0}".format(i[9]))
                    db.commit()
                    print('扩展表状态更新成功!')
                    break
            except:
                print(traceback.format_exc())
                print('接口调用失败,第{0}次重试中...!'.format(str(n_failure_time+1)))
                n_failure_time = n_failure_time + 1
                time.sleep(int(config['send_gap']))
        time.sleep(int(config['send_gap']))
    cr.close()

#查询是否有加载待办任务扩展信息
def get_undone_task_ext(config):
    db=get_db_mysql(config)
    cr=db.cursor()
    sql='''select  count(0)
              from ot_workitem t
              where  t.receiveTime>='2019-07-16 17:0:0'
                 and t.workflowcode REGEXP '^[1-9][0-9][0-9]'
                 and not exists(select 1 from ot_workitem_ext e
                                where e.task_id= t.objectID)
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
       sql='''insert into ot_workitem_ext(inst_id,task_id) 
              select  t.instanceid, t.objectID
              from ot_workitem t
              where  t.receiveTime>='2019-07-16 17:0:0'
                 and t.workflowcode REGEXP '^[1-9][0-9][0-9]'
                 and not exists(select 1 from ot_workitem_ext e
                                where e.task_id= t.objectID)
            '''
       cr.execute(sql)
       db.commit()
       print('采集到新的待办任务!')
    cr.close()

#消息推送
def push(config,debug):
    #循环监听待办任务表变化，变更新扩展表进行消息推送处理
    while True:
      #将未推送消息待办任务写入扩展表
      write_undone_task_ext(config)
      #推送合生通消息
      send_message_hst(config,debug)
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
