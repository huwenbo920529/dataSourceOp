#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 14:35 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : redisConnectPoolDemo.py 
# @Software: PyCharm Community Edition
import redis

REDIS_HOST, REDIS_DB, REDIS_PORT, REDIS_PASSWORD = "", "", "", ""


pool = redis.ConnectionPool(host=REDIS_HOST,
                            port=REDIS_PORT,
                            password=REDIS_PASSWORD,
                            db=REDIS_DB,
                            max_connections=50)


def acquire_lock(key, value, expire=10):
    conn = redis.Redis(connection_pool=pool, decode_responses=True)
    if conn.setnx(key, value):
        conn.expire(key, expire)
        return value
    elif not conn.ttl(key):
        conn.expire(key, expire)

    return False


def release_lock(key, value):
    conn = redis.Redis(connection_pool=pool, decode_responses=True)
    pipe = conn.pipeline(True)
    while True:
        try:
            pipe.watch(key)
            if pipe.get(key) == str(value):
                pipe.multi()
                pipe.delete(key)
                pipe.execute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass

    # we lost the lock
    return False


if __name__ == '__main__':
    ret = acquire_lock("update_pending_list:ubic:1703411", 1, 20)
    print(ret)
    # ret = release_lock("update_pending_list:dash:33108", 1)
