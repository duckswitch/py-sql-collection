# coding: utf-8
"""
This file contains AbstractSerializer class.
"""

from pymongosql.serializer.api_type import (
    Select,
    Insert,
    Field,
    Table,
    Join,
    Update,
    Value,
    Filter,
    Operator,
    Set
)

class ApiSerializer(object):
    """
    Defines the MongoDB Api serializer.
    """

    def __init__(self):
        self._OPERATORS = {
            u"$eq": u"=",
            u"$ne": u"!=",
            u"$gt": u">",
            u"$gte": u">=",
            u"$lt": u"<",
            u"$lte": u"<="
        }
        self.table_columns = {}

    def decode_limit(self, statement, limit):
        statement.limit = limit
        return statement


    def decode_skip(self, statement, skip):
        statement.offset = skip
        return statement

    def generate_table(self, table_name, alias=None, is_root_table=False):
        return Table(
            name=table_name,
            alias=alias or table_name,
            columns=self.table_columns[table_name],
            is_root_table=is_root_table
        )

    def generate_field(self, table, field_name, prefix=None):
        for column in self.table_columns[table.name]:
            if column.name == field_name:
                if table.is_root_table:
                    alias = [column.name]
                else:
                    alias = [prefix, column.name] if prefix else [table.alias or table.name, column.name]

                return Field(
                    column=column,
                    alias=u".".join(alias) or alias,
                    table=table
                )


    def get_available_fields(self, table, prefix=None, to_ignore=None):
        if to_ignore is not None:
            to_ignore = [field.alias for field in to_ignore]
        else:
            to_ignore = []

        fields = []
        for column in table.columns:
            field = self.generate_field(table, column.name, prefix)
            if field.alias not in to_ignore:
                fields.append(field)
        return fields


    def decode_lookup(self, table, lookup):
        joins = []
        for item in lookup:
            from_table = table if u"to" not in item else self.generate_table(
                table_name=item[u"to"],
                alias=item[u"to"]
            )
            to_table = self.generate_table(table_name=item[u"from"])
            joins.append(Join(
                from_table,
                to_table,
                from_field=self.generate_field(from_table, field_name=item[u"localField"]),
                to_field=self.generate_field(to_table, field_name=item[u"foreignField"]),
                as_alias=item[u"as"]
            ))

        return joins

    def decode_projection(self, fields, projection):
        mode = None
        for key, value in projection.items():
            mode = mode or value

            if mode != value:
                raise ValueError(u"All projection field have to use the same value (1 or -1")

            for index, field in reversed(list(enumerate(fields))):
                if (
                            (field.alias != key and field.alias not in projection and mode == 1) or
                            (field.alias in projection and mode == -1)
                ):
                    del fields[index]

        return fields

    def decode_find(self, table, query=None, projection=None, lookup=None):
        """
        Decode a find query to make it understandable by SQL serializer.
        Args:
            query (dict): The query to filter.
            projection (dict): The projection specifies the fields to keep or not.
            lookup (list of dict): List of lookup parameters to make joins. (Not mongoDB compliant, but looks like).

        Returns:
            (Select): A select object ready to be parsed by SQL serializer.
        """

        select = Select()

        select.table = self.generate_table(table_name=table, is_root_table=True)

        if lookup:
            select.joins = self.decode_lookup(select.table, lookup)

        join_tables = []
        fields_to_ignore = []

        for join in select.joins:
            table_names = [table[1].name for table in join_tables] + [select.table.name]
            join_tables += [(join.as_alias, table) for table in [join.from_table, join.to_table] if table.name not in table_names]
            fields_to_ignore += [join.from_field]

        select.fields = self.get_available_fields(select.table, to_ignore=fields_to_ignore)

        for prefix, table in join_tables:
            select.fields += self.get_available_fields(table, prefix, fields_to_ignore)


        if projection:
            select.fields = self.decode_projection(select.fields, projection)

        return select

    def decode_insert_one(self, table_name, document):
        insert = Insert(
            table=self.generate_table(table_name=table_name, is_root_table=True)
        )
        for column in insert.table.columns:
            found = False
            for key, value in document.items():
                if key == column.name:
                    insert.fields.append(Field(insert.table, column))
                    insert.values.append(value)
                    found = True
                    break
            if not found and column.required and column.key != u"pri":
                raise ValueError(u"You must supply a value for field {}.".format(column.name))

        return insert

    def decode_query(self, query, fields, join_operator=u"$and"):
        field_names = [field.alias for field in fields]
        filters = []
        if not isinstance(query, list):
            query = [query]
        for filter in query:
            for key, value in filter.items():
                if key in field_names:

                    field = fields[field_names.index(key)]
                    filters.append(Filter(
                        field,
                        operator=Operator(u"="),
                        value=value
                    ))

        return filters

    def decode_update_set(self, update, fields):
        field_names = [field.alias for field in fields]
        sets = []
        for key in update:
            if key == u"$set":
                for key, value in update[key].items():
                    if key in field_names:
                        field = fields[field_names.index(key)]
                        sets.append(Set(
                            field,
                            value
                        ))

        return sets

    def decode_update_many(self, table_name, query, update, options, lookup=None):
        update_stmt = Update()
        update_stmt.table = self.generate_table(table_name=table_name, is_root_table=True)
        if lookup:
            update_stmt.joins = self.decode_lookup(update_stmt.table, lookup)

        update_stmt.fields = self.get_available_fields(update_stmt.table)
        update_stmt.sets = self.decode_update_set(update, update_stmt.fields)
        update_stmt.filters = self.decode_query(query, fields=update_stmt.fields)
        return update_stmt
