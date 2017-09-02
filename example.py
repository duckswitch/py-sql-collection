# coding: utf-8

import json
from pysqlcollection.client import Client

client = Client(host=u"127.0.0.1", user=u"root", password=u"localroot1234")


sql_collection_test = client.sql_collection_test

with sql_collection_test.transaction() as t:

    sql_collection_test.country.insert_one({
        u"name": u"lala"
    }, in_transaction=t)

LOOKUP = [
  {
    "to": "task",
    "localField": "project_id",

    "from": "project",
    "foreignField": "id",

    "as": "project_id"

  },{
    "to": "project_id",
    "localField": "project_manager_user_id",

    "from": "user",
    "foreignField": "id",

    "as": "project_id.project_manager_user_id"
  },{
    "to": "project_id",
    "localField": "developer_user_id",

    "from": "user",
    "foreignField": "id",

    "as": "project_id.developer_user_id"
  },{
    "to": "project_id",
    "localField": "client_id",

    "from": "client",
    "foreignField": "id",

    "as": "project_id.client_id"
  },{
    "to": "project_id.client_id",
    "localField": "country_id",

    "from": "country",
    "foreignField": "id",

    "as": "project_id.client_id.country_id"
  },{
    "to": "task",
    "localField": "affected_user_id",

    "from": "user",
    "foreignField": "id",

    "as": "affected_user_id"
  }
]

# TEST AUTO LOOKUP

auto_lookup = sql_collection_test.task._auto_lookup(max_deep=3)


print(json.dumps(auto_lookup, indent=4))

cursor = sql_collection_test.task.find(lookup=auto_lookup)
for item in cursor:
    print(json.dumps(item, indent=4))


# INSERT example
sql_collection_test.task.insert_one({
    u"name": u"Making coffee",
    u"project_id": {
        u"id": 1
    },
    u"affected_user_id": {
        u"id": 1
    }
}, lookup=LOOKUP)


# LIST example
cursor = sql_collection_test.task.find(
    query={
        u"$or": [
            {u"affected_user_id.id": 1},
            {u"affected_user_id.id": 2}
        ]
    },
    projection={u"project_id.project_manager_user_id.name": -1}, lookup=LOOKUP
).limit(10).skip(0).sort([(u"affected_user_id.id", -1)])

for item in cursor:
    print(json.dumps(item, indent=4))

# UPDATE example

cursor = sql_collection_test.task.update_many(
    query={
        u"name": u"Making coffee",
        u"project_id.project_manager_user_id.id": 2,
        u"project_id.client_id.id": 1
    },update={
        u"$set": {
            u"name": u"Not making coffee",
            u"project_id.id": 2
        }
    },
    lookup=LOOKUP
)

# DELETE example
cursor = sql_collection_test.task.delete_many(
    query={
        u"name": u"Not making coffee",
        u"project_id.id": 2,
        u"affected_user_id.id": 1
    },
    lookup=LOOKUP
)

