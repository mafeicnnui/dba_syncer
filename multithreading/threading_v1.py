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
        if config[key]=='running':
           print(' '.ljust(3,' ')+str(key).ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def loop(nloop,nsec,config):
    #print('start loop',nloop,'at:',ctime())
    config[str(nloop)] = 'running'
    sleep(nsec)
    #print('loop',nloop,'done at:',ctime())
    config[str(nloop)]='stopped'


def loop2(config):
    start_time = datetime.datetime.now()
    while True:
       if len(config)>0:
          print_dict(config)
       sleep(2)

       if get_seconds(start_time) >= 100:
          print('Timeout thread break!')
          break

def main():
    print('starting at:',ctime())
    threads=[]
    config={}
    loops = [5, 10, 20, 30, 40,50,60]
    max_thread_numbers=3
    nloops=range(len(loops))

    #add thread pool
    for i in nloops:
        config[str(i)]={'status':''}
        t=threading.Thread(target=loop,args=(i,loops[i],config))
        threads.append(t)

    next_thread_id=0
    for i in range(max_thread_numbers):
        threads[i].start()
        next_thread_id=i
    print('next_thread_id=',next_thread_id)

    #output thread
    t=threading.Thread(target=loop2, args=(config,))
    t.start()

    i=0
    while True:

        if i< max_thread_numbers:
            threads[i].join()
            print('thread:{0} terminate!'.format(str(i)))
            i = (i+1) % 3
            if next_thread_id+1<=len(loops)-1:
               threads[next_thread_id+1].start()
               print('start thread:{0} complete!'.format(str(next_thread_id+1)))
               next_thread_id=next_thread_id+1
            else:
               break

    print ('all Done at :',ctime())


if __name__ == "__main__":
     main()
