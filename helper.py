# coding: utf-8

from pymongosql.db import DB
from pymongosql.connection.mysql_connection import MySQLConnection
from pymongosql.serializer.mysql_serializer import MySQLSerializer


db = DB(
    serializer=MySQLSerializer(),
    connection=MySQLConnection(
        host=u"127.0.0.1",
        user=u"root",
        password=u"localroot1234",
        database=u"hours_count"
    )
)

result = db.project.find({})
print(result)