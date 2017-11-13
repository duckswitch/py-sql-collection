# coding: utf-8

import json
from pysqlcollection.client import Client

client = Client(host=u"127.0.0.1", user=u"root", password=u"localroot1234")

sql_collection_test = client.sql_collection_test
# LIST example
cursor = sql_collection_test.country.find(
    query={}
).limit(10).skip(0)

for item in cursor:
    print(item)