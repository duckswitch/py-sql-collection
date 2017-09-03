# coding: utf-8
"""
This file contains MySQLSerializer class.
"""

from .abstract_sql_serializer import AbstractSQLSerializer
from .api_type import (
    Column,
    Field,
    And,
    Or,
    Filter
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

    def encode_joins(self, joins):
        output = [
            u"{} `{}` ON `{}`.{} = `{}`.{}".format(
                join.to_table.name,
                join.to_table.alias,
                join.from_table.alias,
                join.from_field.column.name,
                join.to_table.alias,
                join.to_field.column.name
            )
            for join in joins
            ]
        output = u"LEFT JOIN " + u" LEFT JOIN ".join(output) if len(output) > 0 else u""
        return output

    def encode_filters(self, filters, join_operator=None, is_root=True, is_select=False):
        join_operator = join_operator or u"AND"
        if not isinstance(filters, list):
            filters = [filters]
        where = []
        values = []
        for filt in filters:
            result = None

            if isinstance(filt, Filter):
                operator_value = u"REGEXP" if filt.operator.value == u"regex" else filt.operator.value

                if is_select:
                    where.append(u"`{}` {} %s".format(
                        filt.field.alias,
                        operator_value
                    ))
                else:
                    where.append(u"`{}`.{} {} %s".format(
                        filt.field.table.alias,
                        filt.field.column.name,
                        operator_value
                    ))
                values.append(filt.value)
            elif isinstance(filt, And):
                result = self.encode_filters(filt.filters, u"AND", is_root=False, is_select=is_select)
            elif isinstance(filt, Or):
                result = self.encode_filters(filt.filters, u"OR", is_root=False, is_select=is_select)
            
            if result:
                where += result[0]
                values += result[1]

        if len(where) > 0:
            if is_root:
                where = u" WHERE " + u" {} ".format(join_operator).join(where)
            else:
                return [u"(" + u" {} ".format(join_operator).join(where) + u")"], values
        else:
            where = u""

        return where, values

    def encode_select_count(self, select, with_limit_and_skip=False):
        query, values = self.encode_select(select, with_limit_and_skip=with_limit_and_skip)
        return u"SELECT COUNT(*) FROM ({}) AS A1".format(query), values

    def encode_select(self, select, with_limit_and_skip=True):
        values = []
        fields = [
            u"`{}`.{} AS '{}'".format(
                field.table.alias, field.column.name, field.alias
            ) for field in select.fields]

        joins = self.encode_joins(select.joins)

        # Construct filters
        where, where_values = self.encode_filters(select.filters, is_select=True)
        values += where_values

        # Construct sort
        sort_bindings = {
            1: u"ASC",
            -1: u"DESC"
        }

         limit_offset = u""
        if with_limit_and_skip:
            limit_offset = u"LIMIT %s OFFSET %s"
            values += [select.limit, select.offset]

        sorts = u", ".join([u"`{}` {}".format(sort.field.alias, sort_bindings[sort.direction]) for sort in select.sorts])
        sorts = u"ORDER BY {}".format(sorts) if len(sorts) > 0 else sorts
        query = u"SELECT {} FROM (SELECT * FROM {}) {} {} ".format(u", ".join(fields), select.table.name, select.table.alias, joins)

        displayed = u", ".join([u"`{}`".format(field.alias) for field in select.fields if field.display])
        query = u"SELECT {} FROM ({}) AS A0 {} {} {}".format(displayed, query, where, sorts, limit_offset)
        return query, values

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

    def encode_delete_many(self, delete):
        values = []

        # Construct filters
        # Construct filters
        where, where_values = self.encode_filters(delete.filters)
        values += where_values

        joins = self.encode_joins(delete.joins)

        query = u"DELETE {} FROM {} {} {}".format(
            delete.table.name,
            delete.table.name,
            joins,
            where
        )

        return query, values

    def encode_update_many(self, update):
        """
        Encode Update API object into an SQL query.
        Args:
            update (Update): The Update API object to convert.
        Returns:
            (unicode, list): Query parametes.
        """
        values = []
        # Construct SETS
        sets = []
        for set_stmt in update.sets:
            sets.append(u"{}.{} = %s".format(set_stmt.field.table.name, set_stmt.field.column.name))
            values.append(set_stmt.value)
        sets = u"SET " + u", ".join(sets) if len(sets) > 0 else u""

        # Construct filters
        where, where_values = self.encode_filters(update.filters)
        values += where_values

        joins = self.encode_joins(update.joins)

        query = u"UPDATE {} {} {} {}".format(
            update.table.name,
            joins,
            sets,
            where
        )
        return query, values

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
            typ = u"text"

        return Column(
            name=row[0],
            typ=typ,
            required=(row[2] == u"NO"),
            key=row[3].lower(),
            default=row[4],
            extra=row[5]
        )
