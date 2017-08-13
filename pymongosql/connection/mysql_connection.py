# coding: utf-8
"""
Implement MySQL Connection.
"""

import MySQLdb
from .abstract_connection import AbstractConnection

class MySQLConnection(AbstractConnection):
    """
    Implements low level interactions with MySQL.
    """

    def connect(self):
        """
        Connect to the database. Return a cursor.
        Returns
            (object): The DB Connection.
        """
        kwargs = {
            u"host": self._host,
            u"user": self._user,
            u"passwd": self._password,
            u"charset": u"utf8"
        }
        if self._database:
            kwargs[u"db"] = self._database

        return MySQLdb.connect(**kwargs)

    def execute(self, query, values, return_lastrowid=False):
        """
        Execute a query.
        Args:
            (unicode): The query.
            (list): The values to inject in the query.
        Returns:
            (list, list): Tuple of two : resulting items & result set description.
        """
        # Open connection
        sql_connection = self.connect()
        sql_cursor = sql_connection.cursor()

        # Execute query
        sql_cursor.execute(query, values)

        if return_lastrowid:
            result = sql_cursor.lastrowid
            sql_cursor.connection.commit()
        else:
            result = sql_cursor.fetchall(), sql_cursor.description


        sql_cursor.close()
        sql_connection.close()
        return result