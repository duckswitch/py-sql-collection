# coding: utf-8

import json
from pymongosql.client import Client


client = Client(host=u"127.0.0.1", user=u"root", password=u"localroot1234")

hours_count = client.hours_count

cursor = hours_count.hour.find(
    query={u"id": 1},
    lookup=[
        {
            u"from": u"project",
            u"localField": u"project",
            u"foreignField": u"id",
            u"as": u"project"
        }, {
            u"from": u"client",
            u"to": u"project",
            u"localField": u"client",
            u"foreignField": u"id",
            u"as": u"project.client"
        }
    ]
).limit(1).skip(0)

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
