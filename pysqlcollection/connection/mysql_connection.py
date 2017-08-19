# coding: utf-8
"""
Implement MySQL Connection.
"""

import MySQLdb
from MySQLdb import IntegrityError
from .sql_exception import IntegrityException
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

    def execute(self, query, values, return_lastrowid=False, return_rowcount=False):
        """
        Execute a query.
        Args:
            query (unicode): The query.
            values (list): The values to inject in the query.
            return_lastrowid (bool): Return the field last_rowid from cursor.
            return_rowcount (bool): Return the field rowcount from cursor.

        Returns:
            (list, list): Tuple of two : resulting items & result set description.
        """
        # Open connection
        sql_connection = self.connect()
        sql_cursor = sql_connection.cursor()
        # Execute query
        try:
            sql_cursor.execute(query, values)
        except IntegrityError as e:
            raise IntegrityException(message=e[1])

        if return_lastrowid:
            result = sql_cursor.lastrowid
            sql_cursor.connection.commit()
        elif return_rowcount:
            result = sql_cursor.rowcount
            sql_cursor.connection.commit()
        else:
            result = sql_cursor.fetchall(), sql_cursor.description

        sql_cursor.close()
        sql_connection.close()
        return result