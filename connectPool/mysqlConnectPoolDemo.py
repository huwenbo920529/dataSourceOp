#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 14:29 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : mysqlConnectPool.py 
# @Software: PyCharm Community Edition
from DBUtils.PooledDB import PooledDB
import pymysql


DB_HOST = ''
DB_PORT = ''
DB_USER = ''
DB_PASSWORD = ''
DB_NAME = ''


POOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=100,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=0,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=5,  # 链接池中最多闲置的链接，0和None不限制

    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，
    # 所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    maxshared=3,

    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]

    # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested,
    # 2 = when a cursor is created, 4 = when a query is executed, 7 = always
    ping=7,
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    charset='utf8'
)


def mysql_execute(sql, params_list, commit=False, fetch_tag=None, execute_many=False):

    conn = POOL.connection()
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    try:
        if execute_many:  # 批量操作
            cursor.executemany(sql, params_list)
        else:
            cursor.execute(sql, params_list)
        if commit:
            conn._con.commit()
            conn.close()
            cursor.close()

        elif fetch_tag:
            if fetch_tag == "fetchone":
                res = cursor.fetchone()
                conn.close()
                cursor.close()
                return res
            elif fetch_tag == "fetchall":
                res = cursor.fetchall()
                conn.close()
                cursor.close()
                return res
    except Exception as e:
        conn._con.rollback()
        print("sql={}".format(sql))
        print("params_list={}".format(params_list))
        print(e)
        return False
