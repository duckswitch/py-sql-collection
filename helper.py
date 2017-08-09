# coding: utf-8

from pymongosql.db import DB
from pymongosql.connection.mysql_connection import MySQLConnection
from pymongosql.serializer.sql.mysql_serializer import MySQLSerializer
from pymongosql.serializer.api.mongodb_serializer import MongodbSerializer

db = DB(
    api_serializer=MongodbSerializer(),
    sql_serializer=MySQLSerializer(),
    connection=MySQLConnection(
        host=u"127.0.0.1",
        user=u"root",
        password=u"localroot1234",
        database=u"hours_count"
    )
)

result = db.project.find({u"name": u"kevin"})
print(result)
