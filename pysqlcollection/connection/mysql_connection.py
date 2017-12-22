# coding: utf-8
"""
Implement MySQL Connection.
"""

import MySQLdb
import decimal
from MySQLdb import IntegrityError
from MySQLdb.constants import FIELD_TYPE
from MySQLdb.converters import conversions
from .sql_exception import IntegrityException
from .abstract_connection import AbstractConnection

conversions[FIELD_TYPE.NEWDECIMAL] = decimal.Decimal

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
            u"user": self._user,
            u"passwd": self._password,
            u"charset": u"utf8"
        }
        if self._host:
            kwargs[u"host"] = self._host
        else:
            kwargs[u"unix_socket"] = self._unix_socket

        if self._database:
            kwargs[u"db"] = self._database

        return MySQLdb.connect(**kwargs)

    def to_python_types(self, rows):
        """
        Convert SQL database types coming into Python types.
        Args:
            rows (list of tuples): The request return.

        Returns:
            (list of tuples): The rows parameter converted into python types.
        """
        for line_index, row in enumerate(rows):
            temp = []
            for cell in rows[line_index]:
                if type(cell) is decimal.Decimal:
                    temp.append(float(cell))
                else:
                    temp.append(cell)
            
            rows[line_index] = tuple(temp)
        return rows

    def execute(self, query, values, return_lastrowid=False, return_rowcount=False, sql_cursor=None):
        """
        Execute a query.
        Args:
            query (unicode): The query.
            values (list): The values to inject in the query.
            return_lastrowid (bool): Return the field last_rowid from cursor.
            return_rowcount (bool): Return the field rowcount from cursor.
            sql_cursor: A sql cursor can be use to execute the request.

        Returns:
            (list, list): Tuple of two : resulting items & result set description.
        """
        autocommit = False
        # Open connection
        if not sql_cursor:
            sql_cursor = self.connect().cursor()
            autocommit = True

        # Execute query
        try:
            sql_cursor.execute(query, values)
        except IntegrityError as e:
            raise IntegrityException(message=e[1])

        if return_lastrowid:
            result = sql_cursor.lastrowid

        elif return_rowcount:
            result = sql_cursor.rowcount

        else:

            result = self.to_python_types(list(sql_cursor.fetchall())), sql_cursor.description

        if autocommit and return_lastrowid or return_rowcount:
            sql_cursor.connection.commit()
            sql_cursor.connection.close()
            sql_cursor.close()

        return result
