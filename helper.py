# coding: utf-8

import json
from pysqlcollection.client import Client

client = Client(host=u"127.0.0.1", user=u"root", password=u"localroot1234")

sql_collection_test = client.sql_collection_test


LOOKUP = [
    {
        u"to": u"task",
        u"localField": u"project_id",

        u"from": u"project",
        u"foreignField": u"id",

        u"as": u"project_id"

    },
    {
        u"to": u"project_id",
        u"localField": u"project_manager_user_id",

        u"from": u"user",
        u"foreignField": u"id",

        u"as": u"project_id.project_manager_user_id"
    }, {
        u"to": u"project_id",
        u"localField": u"developer_user_id",

        u"from": u"user",
        u"foreignField": u"id",

        u"as": u"project_id.developer_user_id"
    }, {
        u"to": u"project_id",
        u"localField": u"client_id",

        u"from": u"client",
        u"foreignField": u"id",

        u"as": u"project_id.client_id"
    }, {
        u"to": u"task",
        u"localField": u"affected_user_id",

        u"from": u"user",
        u"foreignField": u"id",

        u"as": u"affected_user_id"
    }
]


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

