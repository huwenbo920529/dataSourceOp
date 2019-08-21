#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 17:51 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : neo4jOpDemo3.py 
# @Software: PyCharm Community Edition
import json
from py2neo import Graph, Node, Relationship, NodeSelector
import pymysql

# Initialize DB Pool
# mysql = Mysql()
mysql = pymysql.connect(host='xxx',
                        port=3306,
                        user='xxx',
                        passwd='xxx',
                        db='xxx',
                        use_unicode=True,
                        charset="utf8")
cursor = mysql.cursor(cursor=pymysql.cursors.DictCursor)

graph = Graph("xxx:7474", username="neo4j", password="xxx")
selector = NodeSelector(graph)
LABEL_PERSON = "Person"
LABEL_PHONE = "Phone"


def load_valid_json(json_str):
    try:
        if isinstance(json_str, dict):
            return json_str
        elif json_str is not None and json_str.strip() != "":
            json_obj = json.loads(json_str)
        else:
            print("Json String is Null to be loaded")
            return None
    except Exception as e:
        print("Load json failed, err = %s" % e.args[0])
        return None
    else:
        # Return the sequence if it is valid
        return json_obj


def get_all_phone_list_from_contacts(contacts):
    all_phone_list = []
    if isinstance(contacts, str):
        contacts_list = load_valid_json(contacts)
    else:
        contacts_list = contacts

    if contacts_list is not None:
        for contact in contacts_list:
            if isinstance(contact.get("phones"), list):
                for phone in contact.get("phones"):
                    if phone is not None and phone.strip():
                        all_phone_list.append(phone)
        else:
            # Remove the duplicated ones
            all_phone_list = list(set(all_phone_list))
            print("All phones list in contacts: %s" % all_phone_list)
            return all_phone_list
    else:
        print("Empty contacts")
        return None


def get_info_from_modle():
    count = 1
    cursor.execute("SELECT max(id) as maxId from t_loan_risk_program")
    max_id = cursor.fetchone().get("maxId")
    print("Max Id(%s) in program table." % max_id)
    range_end = max_id // 100 * 100

    for start in range(range_end, 0, -100):
        end = start + 100
        sql = """SELECT
        customer_id as customerId,
        contacts,
        create_time as createTime
    FROM
        t_loan_risk_program
    WHERE
        id >= %d
    AND id < %d
    AND contacts IS NOT NULL
    AND contacts <> ''""" % (start, end)

        print(sql)
        cursor.execute(sql)

        results = cursor.fetchall()
        print("From %d to %d found %d" % (end, start, len(results)))
        if results:
            for result in results:
                print("TotalNum: %s" % count)
                count += 1
                customer_id = result["customerId"]
                existed_flag = False
                contacts = result["contacts"]
                create_time = str(result["createTime"])
                phone_list = get_all_phone_list_from_contacts(contacts)
                if phone_list:
                    phone_num_in_contacts = len(phone_list)
                else:
                    phone_num_in_contacts = None

                cursor.execute("SELECT cell_phone as cellPhone from t_customer where id = %d" % customer_id)
                cell_phone = cursor.fetchone().get("cellPhone")
                print("Customer:%s cellPhone:%s with %s contacts" % (customer_id, cell_phone, phone_num_in_contacts))

                selected_person = selector.select(LABEL_PERSON, customerId=customer_id)

                for item in selected_person:
                    print("customer Node existed: %s" % item)
                    person_node = item
                    existed_flag = True
                    break

                if customer_id and phone_list:
                    if not existed_flag:
                        person_node = Node(LABEL_PERSON,
                                           customerId=customer_id,
                                           cellPhone=cell_phone,
                                           createTime=create_time,
                                           phoneNumInContacts=phone_num_in_contacts)
                        print("customer Node added: %s" % person_node)
                        graph.create(person_node)

                    phone_count = 1
                    for phone in phone_list:
                        print("PhoneNum: %s" % phone_count)
                        phone_count += 1
                        existed_phone = False
                        selected_phone = selector.select(LABEL_PHONE, phone=phone)

                        for item in selected_phone:
                            print("phone Node existed: %s" % item)
                            phone_node = item
                            existed_phone = True
                            break
                        if not existed_phone:
                            phone_node = Node(LABEL_PHONE, phone=phone)
                            print("Phone Node added: %s" % phone_node)
                            graph.create(phone_node)

                        relation = Relationship(person_node, 'CONTACTS', phone_node)
                        graph.create(relation)
        start -= 100


if __name__ == '__main__':
    get_info_from_modle()
