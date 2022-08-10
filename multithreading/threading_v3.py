#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/7/21 13:29
# @Author : 马飞
# @File : threading_v1.py
# @Software: PyCharm

import threading
from time import sleep,ctime

def exec_sql(tab,task):
    print('starting threading :{}'.format(tab))
    sleep(task['time'])
    print('Table {} running {} seconds'.format(tab, task['time']))


def process_batch(batch):
    threads = []
    for key in batch:
        thread = threading.Thread(target=exec_sql, args=(key, batch[key],))
        threads.append(thread)

    # start threads
    for i in range(0, len(threads)):
        threads[i].start()

    # after all thread end exit func
    # for i in range(0, len(threads)):
    #     threads[i].join()

    print('all batch end at :', ctime())


def main():
    print('starting at:',ctime())

    batch1   = {
        'tab1':{'time':5},
        'tab2':{'time':7},
    }

    batch2 = {
        'tab3': {'time': 1},
        'tab4': {'time': 2},
    }

    process_batch(batch1)
    process_batch(batch2)



if __name__ == "__main__":
     main()
