#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/12/14 11:02
# @Author : 马飞
# @File : sync_mongo_oplog.py
# @Software: PyCharm

from kafka import KafkaConsumer
import json,datetime

'''
  功能：使用Kafka—python的消费模块
'''
class Kafka_consumer():

    def __init__(self, kafkahost, kafkaport, kafkatopic, groupid):
        self.kafkaHost  = kafkahost
        self.kafkaPort  = kafkaport
        self.kafkatopic = kafkatopic
        self.groupid    = groupid
        self.consumer   = KafkaConsumer(self.kafkatopic,
                                        group_id = self.groupid,
                                        bootstrap_servers  = '{}:{}'.format(self.kafkaHost,self.kafkaPort),
                                        auto_offset_reset  = 'earliest',
                                        enable_auto_commit = False)

    def consume_data(self):
        try:
            for message in self.consumer:
                #print json.loads(message.value)
                yield message
        except KeyboardInterrupt as e:
            print(str(e))

'''
  功能：Kafka连接配置
'''
KAFKA_SETTINGS = {
    "host"  : "39.106.184.57", #39.106.184.57
    "port"  : "9092",
    "topic" : 'mallcoo_saledetail_for_ocr' # 猫酷消售数据 - 李毅豪
}

'''
    功能：将datatime类型序列化json可识别类型
'''

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        else:
            return json.JSONEncoder.default(self, obj)


#主函数
def main():
   #获取消费者对象
   consumer = Kafka_consumer(KAFKA_SETTINGS['host'], KAFKA_SETTINGS['port'], KAFKA_SETTINGS['topic'], 'test-consumer-group')
   message  = consumer.consume_data()

   #从topic中获取消息
   for i in message:
       #print('topic={0},json={1}',i.topic,i.value)
       print(i.value.decode())

if __name__ == "__main__":
     main()

