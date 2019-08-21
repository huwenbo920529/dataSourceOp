#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 17:46 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : neo4jOpDemo.py 
# @Software: PyCharm Community Edition
import pandas as pd
from py2neo import Graph, Node, Relationship

graph = Graph('http://localhost:7474',
              username='xxx',
              password="xxx")
test_node_1 = Node('human', label="Person", name="test_node_1")
test_node_2 = Node('human', label="Person", name="test_node_2")
graph.create(test_node_1)
graph.create(test_node_2)

node_1_call_node_2 = Relationship(test_node_1, 'CALL', test_node_2)
node_1_call_node_2['count'] = 1
node_2_call_node_1 = Relationship(test_node_2, 'CALL', test_node_1)
node_2_call_node_1['count'] = 1
graph.create(node_1_call_node_2)
graph.create(node_2_call_node_1)

node_1 = Node('human', type="Asian", name="zhangsan")
node_2 = Node('human', type="American", name="Merry")
graph.create(node_1)
graph.create(node_2)

find_code_one = graph.find_one(label="human",
                               property_key="type",
                               property_value="Asian")
start_one = graph.find_one(label='human',
                           property_key='name',
                           property_value='test_node_1')
end_one = graph.find_one(label='human',
                         property_key='name',
                         property_value='test_node_2')
find_relationship = graph.match_one(start_node=start_one, end_node=end_one, bidirectional=False)
print(find_code_one['type'], find_code_one['name'])
print(find_relationship)

data = pd.DataFrame(graph.data('match(a:human) return a.name'))
print(data)
