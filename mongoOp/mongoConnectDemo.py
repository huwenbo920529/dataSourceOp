#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 14:03 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : mongoConnectDemo.py 
# @Software: PyCharm Community Edition
import datetime
from pymongo import MongoClient


class MongoDB(object):
    _instance = None

    def __init__(self, host=None, port=None, database_name=None, user=None, password=None,
                 collection_name=None, drop_n_create=False):
        try:
            # self._connection = MongoClient(host=host, port=port, maxPoolSize=200)
            # address = 'mongodb://test:"test@101"@{}:{}/{}'.format(host, database_name)
            self._connection = MongoClient(host=host, port=port)
        except Exception as error:
            raise Exception(error)

        if drop_n_create:
            self.drop_db(database_name)

        self._database = None
        self._collection = None

        if database_name:
            self._database = self._connection[database_name]
            self._database.authenticate(user, password)
        if collection_name:
            self._collection = self._database[collection_name]

    @classmethod
    def get_instance(cls):
        if not cls._instance:
            cls._instance = MongoDB()
        return cls._instance

    @staticmethod
    def check_state(obj):
        if not obj:
            return False
        else:
            return True

    def check_db(self):
        if not self.check_state(self._database):
            # validate the database name
            raise ValueError('Database is empty/not created')

    def check_collection(self):
        # validate the collection name
        if not self.check_state(self._collection):
            raise ValueError('Collection is empty/not created')

    def get_overall_details(self):
        # get overall connection information
        client = self._connection
        details = dict((db, [collection for collection in client[db].collection_names()])
                       for db in client.database_names())
        return details

    def get_current_status(self):
        # get current connection information
        return {'connection': self._connection,
                'database': self._database,
                'collection': self._collection}

    def create_db(self, database_name=None):
        # create the database name
        self._database = self._connection[database_name]

    def create_collection(self, collection_name=None):
        # create the collection name
        self.check_db()
        self._collection = self._database[collection_name]

    def get_database_names(self):
        # get the database name you are currently connected too
        return self._connection.database_names()

    def get_collection_names(self):
        # get the collection name you are currently connected too
        self.check_collection()
        return self._database.collection_names(include_system_collections=False)

    def drop_db(self, database_name):
        # drop/delete whole database
        self._database = None
        self._collection = None
        return self._connection.drop_database(str(database_name))

    def drop_collection(self):
        # drop/delete a collection
        self._collection.drop()
        self._collection = None

    def insert(self, post):
        # add/append/new single record
        self.check_collection()
        post_id = self._collection.insert_one(post).inserted_id
        return post_id

    def insert_many(self, posts):
        # add/append/new multiple records
        self.check_collection()
        result = self._collection.insert_many(posts)
        return result.inserted_ids

    def find_one(self, *args):
        # search/find only one matching record
        return self._collection.find_one(*args)

    def find(self, count=False, *args):
        # search/find many matching records
        self.check_collection()
        if not count:
            return self._collection.find(*args)
        # return only count
        return self._collection.find(*args).count()

    def findMany(self, *args):
        return self._collection.find(*args)

    def find_max(self, count_key, *args):
        # search/find many matching records
        self.check_collection()

        return self._collection.find(*args).sort([(count_key, -1)]).limit(1)

    def find_min(self, count_key, *args):
        # search/find many matching records
        self.check_collection()

        return self._collection.find(*args).sort([(count_key, 1)]).limit(1)

    def count(self):
        # get the records count in collection
        self.check_collection()
        return self._collection.count()

    def remove(self, *args):
        # remove/delete records
        return self._collection.remove(*args)

    def update(self, object_id, post):
        # updating/modifying the records
        self.check_collection()
        return self._collection.update({"ObjectId": "{}".format(object_id)}, {"$push": post})
        # pass

    def updateByParams(self, *args):
        self.check_collection()
        return self._collection.update_one(*args)

    def create_index(self):
        # create a single index
        pass

    def create_indexes(self):
        # create multiple indexes
        pass

    def close(self):
        return self._connection.close()

    def get_collection(self, collection_name=None):
        # get collection name
        self.check_db()
        try:
            self._collection = self._database.get_collection(collection_name)
            return self
        except Exception as e:
            return None


if __name__ == '__main__':
    SOURCE_USER_NAME, SOURCE_PASSWORD, SOURCE_CLIENT_ADDRESS, SOURCE_DB_NAME = "", "", "", ""
    table = ""
    source_client = MongoDB(SOURCE_USER_NAME, SOURCE_PASSWORD, SOURCE_CLIENT_ADDRESS, SOURCE_DB_NAME)
    collection = source_client.get_collection(collection_name=table)

    # 1.删除表
    collection.remove({})  # 删除所有数据

    # 2.按条件查询
    d = datetime.datetime.now()
    order_no_list = []
    ret = collection.find(False, {"createTime": {"$lt": d}, "orderNo": {"$in": order_no_list}})[10:20]  # limit查询

    # 3.模糊查询
    collection.find(False, {'company_name': {'$regex': "^{}武汉市.*".format('北京')}})  # col为表名

    # 4.批量插入
    data_list_tmp = []
    collection.insert_many(data_list_tmp)