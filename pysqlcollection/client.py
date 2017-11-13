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
            user,
            password,
            host=None,
            unix_socket=None,
            driver=u"mysql"
    ):

        self._host = host
        self._unix_socket = unix_socket
        self._user = user
        self._password = password
        self._driver = driver
    
    def __getattr__(self, name):
        if name not in self.__dict__:
            self.discover_databases(name)

        return self.__dict__[name]
            
    def discover_databases(self, database_name=None):
        """
        Discover table & views into database.
        Args:
            database_name (unicode): The name of the database to discover.

        """
        if self._driver == u"mysql":
            connection_chain = {
                u"host": self._host,
                u"unix_socket": self._unix_socket,
                u"user": self._user,
                u"password": self._password
            }
            connection = MySQLConnection(**connection_chain)

            api_serializer = ApiSerializer()
            sql_serializer = MySQLSerializer()

            if not database_name:
                databases, _ = connection.execute(*sql_serializer.get_databases())
                databases = [database[0] for database in databases]
            else:
                databases = [database_name]

            for database_name in databases:
                connection_chain = connection_chain.copy()
                connection_chain[u"database"] = database_name
                setattr(self, database_name, DB(
                    api_serializer=api_serializer,
                    sql_serializer=sql_serializer,
                    connection=MySQLConnection(**connection_chain)
                ))
        else:
            raise NotImplemented


