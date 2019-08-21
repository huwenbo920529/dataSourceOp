#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 13:55 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : hiveConnectDemo.py 
# @Software: PyCharm Community Edition
from pyhive import hive

conn = hive.Connection(host='', port=10000, username='', database='')
cur = conn.cursor()
cur.execute('select * from url_log limit 10')
for result in cur.fetchall():
    print(result)
