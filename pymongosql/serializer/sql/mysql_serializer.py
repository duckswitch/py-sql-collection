# coding: utf-8
"""
This file contains MySQLSerializer class.
"""

from .abstract_sql_serializer import AbstractSQLSerializer
from ..api.api_serializer_types import (
    Value,
    Field,
    Operator
)
from ...column import Column

class MySQLSerializer(AbstractSQLSerializer):
    """
    Serialize MySQL requests.
    """

    def query(self, table, query=None):
        query = query or []
        where_stmt = []
        where_values = []
        for item in query:
            if isinstance(item, Value):
                where_stmt.append(u"%s")
                where_values.append(item.value)
            else:
                where_stmt.append(item.value)

        returned = [u"SELECT * FROM {}".format(table)]
        
        if len(where_stmt) > 0:
            returned += [u"WHERE"] + where_stmt

        return u" ".join(returned), where_values

    def get_tables(self):
        """
        Query to get all tables from the database.
        Returns:
            (unicode, list): A query and values to inject in it.
        """
        return "SHOW TABLES", []

    def get_table_columns(self, table):
        """
        Query to get all tables from the database.
        Args:
            table (unicode): The name of the table.
        Returns:
            (unicode, list): A query and values to inject in it.
        """
        return u"DESCRIBE {}".format(table), []

    def interpret_db_column(self, row):
        """
        Receive the result from Database concerning a column, returns
        something the DB class could understand.
        Args:
            row (tuple): A row from a result set describing the column.
        Returns:
            (Column): A column object.
        """
        row_type = row[1]

        if u"int" in row_type or u"double" in row_type:
            typ = u"number"
        elif u"datetime" in row_type:
            typ = u"timestamp"
        else:
            typ = u"string"

        return Column(
            name=row[0],
            typ=typ,
            required=(row[2] == u"NO"),
            key=row[3].lower(),
            default=row[4],
            extra=row[5]
        )
