#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 17:45 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : neo4jOpDemo1.py 
# @Software: PyCharm Community Edition
from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("xxx", "xxx"))


def cyphertx(cypher):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            tx.run(cypher)


cypher = """
            create (Neo:Crew {name:'Neo'}),
                   (Morpheus:Crew {name: 'Morpheus'}),
                   (Trinity:Crew {name: 'Trinity'}),
                   (Cypher:Crew:Matrix {name: 'Cypher'}),
                   (Smith:Matrix {name: 'Agent Smith'}),
                   (Architect:Matrix {name:'The Architect'}),

                   (Neo)-[:KNOWS]->(Morpheus),
                   (Neo)-[:LOVES]->(Trinity),
                   (Morpheus)-[:KNOWS]->(Trinity),
                   (Morpheus)-[:KNOWS]->(Cypher),
                   (Cypher)-[:KNOWS]->(Smith),
                   (Smith)-[:CODED_BY]->(Architect)
         """
cyphertx(cypher)