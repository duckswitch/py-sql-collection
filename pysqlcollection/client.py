# coding: utf-8
"""
This file contains DB class.
"""

from pysqlcollection.serializer.api_serializer import ApiSerializer
from pysqlcollection.serializer.mysql_serializer import MySQLSerializer
from .connection.mysql_connection import MySQLConnection
from .db import DB


class Client(object):
    """
    Serialize MySQL requests.
    """

    def __init__(
            self,
            host,
            user,
            password,
            driver=u"mysql"
    ):

        self._host = host
        self._user = user
        self._password = password
        self._driver = driver
        self.discover_databases()

    def discover_databases(self):
        if self._driver == u"mysql":
            connection_chain = {
                u"host": self._host,
                u"user": self._user,
                u"password": self._password
            }
            connection = MySQLConnection(**connection_chain)

            api_serializer = ApiSerializer()
            sql_serializer = MySQLSerializer()

            databases, _ = connection.execute(*sql_serializer.get_databases())

            for database in databases:
                connection_chain = connection_chain.copy()
                connection_chain[u"database"] = database[0]
                setattr(self, database[0], DB(
                    api_serializer=api_serializer,
                    sql_serializer=sql_serializer,
                    connection=MySQLConnection(**connection_chain)
                ))
        else:
            raise NotImplemented


