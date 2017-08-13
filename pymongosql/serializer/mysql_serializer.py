# coding: utf-8
"""
This file contains MySQLSerializer class.
"""

from .abstract_sql_serializer import AbstractSQLSerializer
from .api_type import (
    Column,
    Field,

)
class MySQLSerializer(AbstractSQLSerializer):
    """
    Serialize MySQL requests.
    """

    def encode_insert(self, insert):

        query = u"INSERT INTO {}({}) VALUES ({})".format(
            insert.table.name,
            u", ".join([field.column.name for field in insert.fields]),
            u", ".join([u"%s"]*len(insert.values))
        )

        return query, insert.values

    def encode_select(self, select):

        values = []
        fields = [u"{}.{} AS '{}'".format(field.table.name, field.column.name, field.alias) for field in select.fields]
        table = u"{} {}".format(select.table.name, select.table.alias)
        joins = [
            u"{} {} ON {}.{} = {}.{}".format(
                join.to_table.name,
                join.to_table.alias,
                join.from_table.name,
                join.from_field.column.name,
                join.to_table.name,
                join.to_field.column.name
            )
            for join in select.joins
        ]
        joins = u" JOIN " + u" JOIN ".join(joins) if len(joins) > 0 else u""
        query = u"SELECT {} FROM {} {} LIMIT %s OFFSET %s".format(u", ".join(fields), table, joins)
        values += [select.limit, select.offset]
        return u"SELECT * FROM ({}) AS A0".format(query), values

    def get_relations(self, database_name, table_name):

        query = u"""
                SELECT
                TABLE_NAME AS 'table_name',
                COLUMN_NAME AS 'column_name',
                REFERENCED_TABLE_NAME AS 'referenced_table_name',
                REFERENCED_COLUMN_NAME AS 'referenced_column_name'
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND REFERENCED_TABLE_NAME IS NOT NULL
                """

        return query, [database_name, table_name]

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
