#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 17:27 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : mongoOpDemo.py 
# @Software: PyCharm Community Edition
from pymongo import MongoClient
conn = MongoClient('mongodb://localhost:27017/')#或conn = MongoClient('localhost', 27017)
db = conn.testdb  # testdb为数据库名称

# 1.插入数据（insert插入一个列表多条数据不用遍历，效率高， save需要遍历列表，一个个插入）
db.col.insert({"name": 'yanying', 'province': '江苏', 'age': 25})
# 或db.col.save({"name":'yanying','province':'江苏','age':25})
db.col.insert([
    {"name": 'yanying', 'province': '江苏', 'age': 25},
    {"name": '张三', 'province': '浙江', 'age': 24},
    {"name": '张三1', 'province': '浙江1', 'age': 25},
    {"name": '张三2', 'province': '浙江2', 'age': 26},
    {"name": '张三3', 'province': '浙江3', 'age': 28},
])

# 2.查询数据（查询不到则返回None）
# 注意：find_one返回的是<class 'dict'>，而find返回的是<class 'pymongo.cursor.Cursor'>
print('\n查询数据：')
cursor = db.col.find_one()  # col为表名
print(cursor)
for item in db.col.find({'name': 'yanying'}):
    print(item)

# 3.更新数据
print('\n更新数据：')
db.col.update({'name': '张三1'}, {'$set': {'province': '北京'}})
print(db.col.find_one({'name': '张三1'}))

# 4.删除数据
print('\n删除数据：')
db.col.remove({'name': '张三3'})
print(db.col.find_one({'name': '张三3'}))

# 5.mongodb的条件操作符
#    (>)  大于 - $gt
#    (<)  小于 - $lt
#    (>=)  大于等于 - $gte
#    (<= )  小于等于 - $lte
#   (!=) 不等于 - $ne
print('\n条件操作符：')
for item in db.col.find({"age": {"$gt": 25}}, {"name": 1, 'age': 1}):
    print(item)

# 5.type(判断类型)
# 找出name的类型是String的
print('\n类型判断：')
for i in db.col.find({'name': {'$type': 2}}):
    print(i)

# 6.排序
# 在MongoDB中使用sort()方法对数据进行排序，sort()方法可以通过参数指定排序的字段，
# 并使用 1 和 -1 来指定排序的方式，其中 1 为升序，-1为降序。
print('\n排序：')
for item in db.col.find().sort([('age', 1), ('name', -1)]):
    print(item)

# 7.limit和skip
# limit()方法用来读取指定数量的数据
# skip()方法用来跳过指定数量的数据
print('\nlimit和skip：')
for item in db.col.find().skip(1).limit(2):
    print(item)

# 8.IN用法
# 找出age是24、25的数据
print('\nIN用法：')
for item in db.col.find({"age": {"$in": (24, 25)}}):
    print(item)

# 8.OR用法
# 找出age是24或26的数据
print('\nOR用法：')
for item in db.col.find({"$or": [{"age": 24}, {"age": 26}]}):
    print(item)

# 9.ALL用法
# dic = {"name": "lisi", "age": 18, "li": [1, 2, 3]}
# dic2 = {"name": "zhangsan", "age": 18, "li": [1, 2, 3, 4, 5, 6]}
# db.col.insert(dic)
# db.col.insert(dic2)
print('\nALL用法：')
for item in db.col.find({'li': {"$all": [1, 2, 3, 4]}}):
    print(item)

# 10.push/pushAll用法
# push 添加一个元素
print('\nPush：')
db.col.update({'name': 'lisi'}, {'$push': {'li': 4}})
print(db.col.find_one({'name': 'lisi'}))
# pushAll 添加多个元素
print('\npushAll：')
db.col.update({'name': 'lisi'}, {'$pushAll': {'li': [5, 6, 7]}})
print(db.col.find_one({'name': 'lisi'}))

# 11.pop/pull/pullAll用法
# pop移除最后一个元素(-1为移除第一个)
print('\nPOP：')
db.col.update({'name': "lisi"}, {'$pop': {'li': 1}})
for item in db.col.find({'name': 'lisi'}):
    print(item)
# pull （按值移除）
print('\nPULL：')
db.col.update({'name': "lisi"}, {'$pull': {'li': 1}})
for item in db.col.find({'name': 'lisi'}):
    print(item)
# pullAll （移除全部符合条件的）
print('\nPullAll：')
db.col.update({'name': 'lisi'}, {'$pullAll': {'li': [1, 2, 3]}})
for item in db.col.find({'name': 'lisi'}):
    print(item)
