#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/3/16 14:15
# @Author : 马飞
# @File : file_upload.py
# @Software: PyCharm

# import sys
# def is_number(s):
#     try:
#         int(s)
#         return True
#     except ValueError:
#         return False
#
# for line in sys.stdin:
#    a= line.split()
#    if is_number(a[0]):
#        print('YES')
#    else:
#        print('NO')
#

# def is_number(s):
#     try:
#         int(s)
#         return True
#     except ValueError:
#         return False
#
# a= input()
# if is_number(a):
#    print('YES')
# else:
#    print('NO')

def is_number(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def isperfectNumber(n):
    a = 1
    b = n
    s = 0
    while a < b:
        if n % a ==0:
            s += a + b
        a  += 1
        b = n/a
    if a ==b and a*b == n:
        s +=a
    return  s - n == n

a= input()
if is_number(a):
    if isperfectNumber(int(a)):
         print(int(a))
         print('YES')
    else:
         print('NO')
else:
   print('NO')

print('2-1000之间的完全数')
print('---------------------------')
for k in range(2,1000):
    if isperfectNumber(k):
        print(k)