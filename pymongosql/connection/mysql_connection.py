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
        self.sql_connection = MySQLdb.connect(
            host=self._host,
            user=self._user,
            passwd=self._password,
            db=self._database,
            charset=u"utf8"
        )
        self.sql_cursor = None

    def execute(self, conn, query, values):
        """
        Execute a query.
        Args:
            (unicode): The query.
            (list): The values to inject in the query.
        Returns:
            (list, list): Tuple of two : resulting items & result set description.
        """
        # Open connection
        self.sql_connection = self.connect()
        self.sql_cursor = self.sql_connection.cursor()

        # Execute query
        self.sql_cursor.execute(query, values)
        return  self.sql_cursor
