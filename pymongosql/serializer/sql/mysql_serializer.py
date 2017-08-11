# coding: utf-8
"""
This file contains MySQLSerializer class.
"""

from .abstract_sql_serializer import AbstractSQLSerializer
from ..api.api_serializer_types import (
    Value,
    Operator
)
from pymongosql.serializer.api.api_serializer_types import Column

class MySQLSerializer(AbstractSQLSerializer):
    """
    Serialize MySQL requests.
    """

    def query(self, statement):
        where_stmt = []
        where_values = []
        for item in statement.where:
            if isinstance(item, Value):
                where_stmt.append(u"%s")
                where_values.append(item.value)
            elif isinstance(item, Column):
                where_stmt.append(item.name)
            else:
                where_stmt.append(item.value)


        values = where_values
        fields_statements = [u"*"]
        if len(statement.fields) > 0:
            fields_statements = [column.name for column in statement.fields]

        returned = [u"SELECT {} FROM {}".format(u", ".join(fields_statements), statement.table.value)]
        
        if len(where_stmt) > 0:
            returned += [u"WHERE"] + where_stmt

        if statement.sorts:
            binding = {
                1: u"ASC",
                -1: u"DESC"
            }

            sort_stmt = [ u"{} {}".format(sort.value[0], binding[sort.value[1]]) for sort in statement.sorts]
            returned.append(u"ORDER BY {}".format(u", ".join(sort_stmt)))

        if statement.limit is not None:
            returned.append(u"LIMIT %s")
            values.append(statement.limit.value)

            if statement.offset is not None:
                returned.append(u"OFFSET %s")
                values.append(statement.offset.value)

        print(" ".join(returned))
        return u" ".join(returned), values

    def get_tables(self):
        """
        Query to get all tables from the database.
        Returns:
            (unicode, list): A query and values to inject in it.
        """
        return u"SHOW TABLES", []

    def get_databases(self):
        """
        Query to get all tables from the database.
        Returns:
            (unicode, list): A query and values to inject in it.
        """
        return u"SHOW DATABASES", []

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
