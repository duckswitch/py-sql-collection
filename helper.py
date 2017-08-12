# coding: utf-8

from pymongosql.client import Client


client = Client(host=u"127.0.0.1", user=u"root", password=u"localroot1234")

hours_count = client.hours_count

cursor = hours_count.project.find(
    query={u"name": u"BTS"},
    projection={u"name": 1},
    lookup=[
        {
            u"from": u"client",
            u"localField": u"client",
            u"foreignField": u"id",
            u"as": u"client"
        }
    ]
).limit(5).skip(0)
for item in cursor:
    print(item)
