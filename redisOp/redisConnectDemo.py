#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 13:58 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : redisConnectDemo.py 
# @Software: PyCharm Community Edition
import redis

pool = redis.ConnectionPool(host='xxx', port=6379, password='xxx')

r = redis.Redis(connection_pool=pool)
# pipe = r.pipeline()
# pipe.set('one', 'first')
# pipe.set('two', 'second')
# pipe.execute()
# r.set('flask_project', 'customer_info_portal')
# print r.get('flask_project')
# for k in r.keys():
#     r.delete(k)

# r.delete('{"username": "hu", "birth_date": "199205"}')


print(r.keys())
# r.delete('flask_project')
print(r.get('l_words_scala'))
# for k in r.keys():
#     r.get(k)
