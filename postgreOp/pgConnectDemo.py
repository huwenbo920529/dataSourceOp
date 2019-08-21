#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 13:53 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : pgConnectDemo.py 
# @Software: PyCharm Community Edition
import psycopg2


def get_collection_from_pg():
    conn = psycopg2.connect(host="xxx", port="xxx", dbname="xxx",
                            user="xxx", password="xxx")
    cur = conn.cursor()
    cur.execute("select value from t_configs where type=6 and valid_flag = 1 ")
    rows = cur.fetchall()
    tmp = []
    for item in rows:
        if item[0] not in ["installmentOperator", "installmentProgram"]:
            tmp.append(item[0])
    return tmp

variable_table_list = get_collection_from_pg()
print(variable_table_list)
