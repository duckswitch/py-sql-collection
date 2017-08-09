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
        return MySQLdb.connect(
            host=self._host,
            user=self._user,
            passwd=self._password,
            db=self._database,
            charset=u"utf8"
        )

    def execute(self, query, values):
        """
        Execute a query.
        Args:
            (unicode): The query.
            (list): The values to inject in the query.
        Returns:
            (list, list): Tuple of two : resulting items & result set description.
        """
        # Open connection
        conn = self.connect()
        cursor = conn.cursor()

        # Execute query
        cursor.execute(query, values)
        result = cursor.fetchall(), cursor.description

        # Commit, close & return.
        cursor.connection.commit()
        cursor.close()
        conn.close()
        return result
