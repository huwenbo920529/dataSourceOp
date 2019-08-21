#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 14:24 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : mysqlConnectDemo.py 
# @Software: PyCharm Community Edition
import pymysql


def get_collection_from_mysql(host, port, db, user, password):
    conn = pymysql.connect(host=host, port=port, db=db, user=user, password=password)
    cur = conn.cursor()
    cur.execute("select value from t_configs where type=6 and valid_flag = 1 ")
    rows = cur.fetchall()
    tmp = []
    for item in rows:
        if item[0] not in ["installmentOperator", "installmentProgram"]:
            tmp.append(item[0])
    return tmp


if __name__ == '__main__':
    HOST, PORT, DB, USER, PASSWORD = "", "", "", "", ""
    variable_table_list = get_collection_from_mysql(HOST, PORT, DB, USER, PASSWORD)
