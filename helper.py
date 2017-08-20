# coding: utf-8

import json
from pysqlcollection.client import Client


client = Client(host=u"127.0.0.1", user=u"root", password=u"localroot1234")

hours_count = client.hours_count


description = hours_count.project.get_description(lookup=[
    {
        u"from": u"client",
        u"localField": u"client",
        u"foreignField": u"id",
        u"as": u"banane"
    }
])
cursor = hours_count.hour.find(
    query={
        "affected_to.email": u"admin@myapp.net"
    },
    auto_lookup=2
).limit(5).skip(0).sort([(u"id", 1)])
#

for item in cursor:
    print(json.dumps(item, indent=4))


result = hours_count.hour.insert_one({
  "affected_to": {
    "email": "admin@myapp.net",
    "id": 22,
    "name": "Admin"
  },
  "comments": None,
  "issue": "",
  "minutes": 0,
  "project": {
    "client": {
      "id": 4,
      "name": "Valeo"
    },
    "code": None,
    "id": 4,
    "name": "Is people efficiency",
    "provisioned_hours": 240,
    "started_at": 1503088575
  },
  "started_at": 1503175288
}, auto_lookup=1)


result = hours_count.project.update_many(query={
    u"name": u"TEST2"
}, update={
    u"$set": {
        u"client": {
            u"id": 1,
            u"name": u"Kiloutou"
        }
    }
}, auto_lookup=3)





cursor = hours_count.hour.find(
    projection={
        u"project.name": 1
    },
    auto_lookup=2
).limit(1).skip(0)

for item in cursor:
    print(json.dumps(item, indent=4))






#
# print(result.inserted_id)
#
result = hours_count.project.update_many(query={
    u"id": 1
}, update={
    u"$set": {
        u"started_at": 1503088575
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
