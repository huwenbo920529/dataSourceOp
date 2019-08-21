#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 17:48 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : neo4jDemo2.py 
# @Software: PyCharm Community Edition
from py2neo import Node, Relationship, Graph

graph = Graph('http://localhost:7474',
              username='xxx',
              password="xxx")
# tx = graph.begin()
# a = Node("Person", name="Alice")
# tx.create(a)
# b = Node("Person", name="Bob")
# ab = Relationship(a, "KNOWS", b)
# tx.create(ab)
# tx.commit()
# print(graph.exists(ab))

data = graph.data("""CREATE (a:Person { name:"Tom Hanks",born:1956 })-[r:ACTED_IN { roles: ["Forrest"]}]->(m:Movie { title:"Forrest Gump",released:1994 })
            CREATE (d:Person { name:"Robert Zemeckis", born:1951 })-[:DIRECTED]->(m)
            RETURN a,d,r,m""")
print(data)
