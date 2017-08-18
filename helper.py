# coding: utf-8

import json
from pysqlcollection.client import Client


client = Client(host=u"127.0.0.1", user=u"root", password=u"localroot1234")

hours_count = client.hours_count

cursor = hours_count.hour.find(
    query={},
    lookup=[
        {
            u"from": u"project",
            u"localField": u"project",
            u"foreignField": u"id"
        }
    ]
).limit(1).skip(0).sort([(u"id", 1)])

for item in cursor:
    print(json.dumps(item, indent=4))

cursor = hours_count.hour.find(
    projection={
        u"project.name": 1
    },
    auto_lookup=2
).limit(1).skip(0)

for item in cursor:
    print(json.dumps(item, indent=4))




result = hours_count.hour.insert_one({
    u"started_at": u"2017-05-01",
    u"minutes": 15,
    u"project": {
        u"id": 4
    },
    u"affected_to": 1
}, auto_lookup=2)


print(result.inserted_id)

result = hours_count.project.update_many(query={
    u"banane.name": u"TEST 2"
}, update={
    u"$set": {
        u"name": u"PATATE"
    }
}, options={
    u"upsert": False,
    u"multi": False
}, lookup=[
    {
        u"from": u"client",
        u"localField": u"client",
        u"foreignField": u"id",
        u"as": u"banane"
    }
])


#
result = hours_count.project.delete_many(query={
    u"banane.name": u"TEST 2"
}, lookup=[
    {
        u"from": u"client",
        u"localField": u"client",
        u"foreignField": u"id",
        u"as": u"banane"
    }
])
#
print(result.deleted_count)
