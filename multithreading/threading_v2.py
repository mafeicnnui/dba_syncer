#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/7/21 13:29
# @Author : 马飞
# @File : threading_v1.py
# @Software: PyCharm

import threading
import datetime
from time import sleep,ctime

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
        print(' '.ljust(3,' ')+str(key).ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def print_dict2(config):
    x=''
    for key in config:
        #print(' '.ljust(3,' ')+str(key).ljust(20,' ')+'=',config[key])
        y=' '.ljust(3,' ')+str(key).ljust(20,' ')+' = '+str(config[key])
        x=x+'{0}'.format(y)+'\n'

    v='''
        {0}
        {1}
        {2}
        (3)
      '''.format('-'.ljust(125,'-'),
                 ' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value',
                 '-'.ljust(125, '-'),
                 x,
                 '-'.ljust(125, '-'))

    print('{0}'.format(v))

def loop(name,config):
    #print(config)
    config[name]['status'] = 'running'
    sleep(config[name]['time'])
    config[name]['status'] = 'stopped'

def loop2(config):
    print('print thread running status...')
    while True:
       i_counter = 0
       for key in list(config.keys()):
           if config[key]['status'] == 'stopped':
              i_counter=i_counter+1

           if config[key]['status']=='running':
              print_dict(config)
              sleep(3)

       if i_counter==len(config):
          print('all thread end,exit!')
          break

def main():
    print('starting at:',ctime())
    config  = {}
    threads = []
    loops   = [5, 10, 20, 30,40,50,60]
    nloops  = range(len(loops))

    #add thread pool
    for i in nloops:
        name         = 'thread' + str(i)
        config[name] = {'time': loops[i], 'status': 'init'}
        thread       = threading.Thread(target=loop,args=(name,config,))
        threads.append(thread)

    #monitor thread
    t = threading.Thread(target=loop2, args=(config,))
    t.start()

    #first running three threads
    for i in range(0,3):
        threads[i].start()

    #runnint others threads,max three
    i=3
    while True:
        for key in list(config.keys()):
            if config[key]['status'] == 'stopped':
               threads[i].start()
               del config[key]
               i=i+1
        sleep(1)
        if i==len(loops):
           break


    for i in nloops:
        threads[i].join()
    print ('all Done at :',ctime())

    print('print config...')
    print_dict(config)

if __name__ == "__main__":
     main()
