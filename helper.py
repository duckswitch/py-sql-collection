# coding: utf-8

from pymongosql.client import Client


client = Client(host=u"127.0.0.1", user=u"root", password=u"localroot1234")

hours_count = client.hours_count
cursor = hours_count.project.find(query={u"name": u"BTS"}, projection={u"name": 1}).limit(5).skip(0)


print(cursor)
for item in cursor:
    print(item)
# cursor = db.user.find({})
# for item in cursor:
#     print(item)