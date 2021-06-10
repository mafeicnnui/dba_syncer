#!/usr/bin/env pythonx
# -*- coding: utf-8 -*-
# @Time    : 2018/6/2 10:48
# @Author  : 马飞
# @File    : h3bpm_interface_easylife.py
# @Software: PyCharm

import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver
import tornado.locale
from   tornado.options  import define, options
import json
import sys
import smtplib
import configparser
import warnings
from email.mime.text import MIMEText

def send_mail465(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    result = {}
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
        result['code'] = 200
        result['msg'] = '保存成功！'
    except smtplib.SMTPException as e:
        result['code'] = -1
        result['msg'] = '保存失败！'
    return result

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def init():
    config = {}
    config['mail_title'] = '[合生通]H3-BPM消息通知'
    config['send_user']  = '190343@lifeat.cn'
    config['send_pass']  = 'Hhc5HBtAuYTPGHQ8'
    config['acpt_user']  = '190343@lifeat.cn'
    return config

class h3bpm_hopson_send(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        try:
            v_message = self.get_argument("message")
            print('interface:v_message=', v_message)
        except:
            print('get method!')
            self.write('''
请求方法不正确！
请求方法:POST
请求参数示例如下：
{
    'account': '13962129807797',
    'accountName': '翠翠',
    'code':'3a89dcb7-a823-41e3-b17c-d1a077f9bf54',
    'applicantPerson':小米之家,
    'applicantTime': 2019-06-12 05:20:20,
    'beginTime':2019-06-12 05:20:20,
    'endTime': 2019-06-12 05:20:20,
    'content': '测试请求参数'
}

''')

    def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        config            = init()
        v_message         = self.get_argument("message")
        print('interface:v_message=', v_message)
        d_message         = json.loads(v_message)
        v_account         = d_message['account']
        v_bindingType     = d_message['bindingType']
        v_code            = d_message['code']
        v_applicantPerson = d_message['applicantPerson']
        v_applicantTime   = d_message['applicantTime']
        v_beginTime       = d_message['beginTime']
        v_endTime         = d_message['endTime']
        v_content         = d_message['content']

        v_contents_header='''
<html>
      <head>
           <style type="text/css">
               .xwtable {width: 60%;border-collapse: collapse;border: 1px solid #ccc;}
               .xwtable thead td {font-size: 12px;color: #333333;
                                  text-align: left;border: 1px solid #ccc; font-weight:bold;}
               .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
               .xwtable tbody tr.alt-row {background: #f2f7fc;}
               .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
           </style>
      </head>
<body>'''
        v_contents_body  ='''
<br><span>{0},您有新的工单等待您审批,请及时处理！</span>
    <br>工单信息如下:<br>
    <table class='xwtable'>
      <tr>
        <td width=35%>账号:</td><td>{1}</td>
      </tr>
      <tr>
        <td>流程编码:</td><td>{2}</td>
      </tr>
      <tr>
        <td>申请商户名称:</td><td>{3}</td>
      </tr>
      <tr>
        <td>申请时间:</td><td>{4}</td>
      </tr>
      <tr>
        <td>开始时间:</td><td>{5}</td>
      </tr>
      <tr>
        <td>结束时间:</td><td>{6}</td>
      </tr>
      <tr>
        <td>内容:</td><td>{7}</td>
      </tr>
    </table>
    <p>
    测试接口地址如下:<br>
    <hr/>
    &nbsp;<span>合生通：http://10.2.39.40:7878/h3bpm_hopson_send</span><br>
    &nbsp;<span>好房:开发中...</span>
</body>
</html>
'''.format(v_account,
           v_account,
           v_code,
           v_applicantPerson,
           v_applicantTime,
           v_beginTime,
           v_endTime,
           v_content
           )
        v_contents=v_contents_header+v_contents_body
        d_list=send_mail465(config['send_user'],
                            config['send_pass'],
                            config['acpt_user'],
                            config['mail_title'],v_contents)
        v_json = json.dumps(d_list)
        print(v_json)
        self.write(v_json)

define("port", default=7878, help="run on the given port", type=int)
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/h3bpm_hopson_send", h3bpm_hopson_send),
        ]
        tornado.web.Application.__init__(self, handlers)
        print('Interface hopson Server running...')

if __name__ == '__main__':
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
