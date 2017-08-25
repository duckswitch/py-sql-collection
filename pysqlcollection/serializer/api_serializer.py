# coding: utf-8
"""
This file contains AbstractSerializer class.
"""

import json
from pysqlcollection.serializer.api_type import (
    Select,
    Insert,
    Field,
    Table,
    Join,
    Update,
    Delete,
    Filter,
    Operator,
    Set,
    Or,
    And,
    Sort
)
from datetime import datetime
from .api_exception import (
    WrongParameter,
    MissingField,
    BadRequest
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
            u"$lte": u"<=",
            u"$regex": u"regex"
        }
        self._RECURSIVE_OPERATORS = {
            u"$and": And,
            u"$or": Or
        }
        self.table_columns = {}

    def decode_limit(self, statement, limit):
        """
        Add a limit to a Select statement.
        Args:
            statement (Select): The statement to update.
            limit (int): How much the limit is.
        Returns:
            (Select): The updated statement.
        """
        if not isinstance(limit, int) or limit < 0:
            raise WrongParameter(
                u"Limit must be greater or equal to 0."
            )
        statement.limit = limit
        return statement

    def decode_sort(self, statement, key_or_list, direction=None):
        """
        Applies a sort on the cursor and inject it in the statement object.
        Args:
            key_or_list (unicode or list of tuple): Field to sort or sort list for many fields.
            direction (int): Direction if only one key supplied to the sort.
        Return:
            (Cursor): The updated cursor.
        """
        if not isinstance(key_or_list, list):
            key_or_list = [(key_or_list, direction)]

        field_names = [field.alias for field in statement.fields]
        
        sorts = []
        for key, direction in key_or_list:
            if direction not in [1, -1]:
                raise WrongParameter(u"You can only sort direction with '1' (ASC) or '-1' (DESC).")

            if key in field_names:            
                sorts.append(
                    Sort(
                        field=statement.fields[field_names.index(key)],
                        direction=direction
                    )
                )
            else:
                raise WrongParameter(u"Field '{}' can't be sorted because it doesn't exist.".format(key))
          
        statement.sorts = sorts
        return statement

    def decode_skip(self, statement, skip):
        """
        Add a skip to a Select statement.
        Args:
            statement (Select): The statement to update.
            skip (int): How much the limit is.
        Returns:
            (Select): The updated statement.
        """
        if not isinstance(skip, int) or skip < 0:
            raise WrongParameter(
                u"Skip must be greater or equal to 0."
            )
        statement.offset = skip
        return statement

    def generate_table(self, table_name, alias=None, is_root_table=False):
        """
        Generate a table representation 
        regarding parameters and table_columns in memory.
        Args:
            table_name (unicode): The name of the table to generate.
            alias (unicode): The alias we could use to call the table.
            is_root_table (bool): Used when there are JOINS, to know if it is
                the main table or not.
        Returns:
            (Table): The generated table.
        """
        
        return Table(
            name=table_name,
            alias=alias or table_name,
            columns=self.table_columns[table_name],
            is_root_table=is_root_table
        )

    def generate_field(self, table, field_name, prefix=None):
        """
        Generate a field used in where or select.
        Args:
            table (Table): The table the field comes from.
            field_name (unicode): The name we want to give to the field.
            prefix (unicode): A prefix to apply to the field alias.
        Returns:
            (Field): The generated field.
        """
        # For each column in the memory representation
        # of the table.
        for column in self.table_columns[table.name]:
            # If the column name matches the field we want
            # to create.
            if column.name == field_name:
                # Check if root table, then alias is directly the name
                # (it will be there once).
                if table.is_root_table:
                    alias = [column.name]
                else:
                    # Else it can be used several time. Prefix is important.
                    alias = [
                        prefix, column.name
                    ] if prefix else [table.alias or table.name, column.name]

                return Field(
                    column=column,
                    alias=u".".join(alias) or alias,
                    table=table
                )


    def get_available_fields(self, table, prefix=None, to_ignore=None):
        """
        Get a table, and look for all available fields inside.
        Args:
            table (Table): The table to prospect.
            prefix (unicode): A potential prefix to apply on the resulting field.
            to_ignore (list of Field): Field to ignore if we find them.
        Returns:
            (list of Field): The resulting fields coming from the table.
        """
        if to_ignore is not None:
            to_ignore = [
                field.alias or field.column.name for field in to_ignore
            ]
        else:
            to_ignore = []

        fields = []
        for column in table.columns:
            field = self.generate_field(table, column.name, prefix)

            if field.alias not in to_ignore:
                fields.append(field)

        return fields


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
                    fields[index].display = False

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

        select.fields = self.get_available_fields(select.table)

        select = self._decode_joins(select, lookup)

        if projection:
            select.fields = self.decode_projection(select.fields, projection)

        if query:
            select.filters = self.decode_query(query, fields=select.fields)

        return select

    def decode_insert_one(self, table_name, document, lookup=None):
        lookup = lookup or []
        document = self.json_to_one_level(document)
        insert = Insert(
            table=self.generate_table(table_name=table_name, is_root_table=True)
        )
        # Replace lookups
        if len(lookup) > 0:
            for index, (key, value) in enumerate(document.items()):
                splitted = key.split(u".")
                if len(splitted) > 1:
                    for look in lookup:

                        if look.get(u"as") == splitted[0] and look.get(u"foreignField") == splitted[1]:
                            del document[key]
                            document[look.get(u"localField")] = value
                            break

        for column in insert.table.columns:
            found = False

            for key, value in document.items():

                if key == column.name:

                    insert.fields.append(Field(insert.table, column))
                    insert.values.append(self.cast_value(column.type, value))
                    found = True
                    break

            if not found and (column.required and column.extra != u"auto_increment"):
                raise MissingField(u"You must supply a value for field '{}'.".format(column.name))

        return insert

    def cast_value(self, column_type, value):
        if column_type in [u"timestamp"] and (isinstance(value, int) or isinstance(value, float)):
            value = datetime.utcfromtimestamp(value)

        return value

    def decode_query(self, query, fields, parent=None, join_operator=None):
        join_operator = join_operator or And
        field_names = [field.alias for field in fields]
        filters = []
        if not isinstance(query, list):
            query = [query]

        for filt in query:
            for key, value in filt.items():
                if key in field_names:

                    if isinstance(value, dict):
                        filters += [self.decode_query(value, fields, parent=key)]
                    else:
                        field = fields[field_names.index(key)]
                        operator = Operator(u"=")

                        filters.append(Filter(
                            field,
                            operator,
                            value=self.cast_value(field.column.type, value)
                        ))

                elif key in self._OPERATORS:

                    field = fields[field_names.index(parent)]
                    operator = Operator(self._OPERATORS[key])
                    filters.append(Filter(
                        field,
                        operator,
                        value=self.cast_value(field.column.type, value)
                    ))

                elif key in self._RECURSIVE_OPERATORS and isinstance(value, list):
                    filters += [
                        self.decode_query(value, fields, join_operator=self._RECURSIVE_OPERATORS[key])
                    ]

        return join_operator(filters)

    def decode_update_set(self, table, update, fields):
        field_names = [field.alias for field in fields]
        sets = []
        for key in update:
            if key == u"$set":
                update[key] = self.json_to_one_level(update[key])
                for key, value in update[key].items():
                    if key in field_names:
                        field = fields[field_names.index(key)]
                        if field.table.name == table.name:
                            sets.append(Set(
                                field,
                                self.cast_value(field.column.type, value)
                            ))

        return sets

    def decode_update_many(self, table_name, query, update, options, lookup=None):
        update_stmt = Update()
        update_stmt.table = self.generate_table(table_name=table_name, is_root_table=True)

        update_stmt.fields = self.get_available_fields(update_stmt.table)

        update_stmt = self._decode_joins(update_stmt, lookup)

        update_stmt.sets = self.decode_update_set(update_stmt.table, update, update_stmt.fields)
        update_stmt.filters = self.decode_query(query, fields=update_stmt.fields)
        if len(update_stmt.filters.filters) == 0:
            raise BadRequest(u"You need to supply at least one filter.")


        return update_stmt

    def _merge_fields(self, fields, to_merge):
        output = []
        for field in fields:
            merged = False
            for index, field_to_merge in enumerate(to_merge):

                if field_to_merge.alias.startswith(field.alias):

                    output.append(to_merge[index])
                    del to_merge[index]
                    merged = True
                    break

            if not merged:
                output.append(field)

        output = output + to_merge
        return output + to_merge

    def decode_lookup(self, table, lookup):
        """
        Test decode lookup.
        Args:
            table (Table): The table where we look for joins.
            lookup (dict): The lookup representation as given to the methods.
        Returns:
            (list of Join): Joins representations resulting.
        """
        joins = []
        for look in lookup:
            try:
                from_table = self.generate_table(look.get(u"to"), alias=look.get(u"to"))
            except KeyError:
                # To is not a table. Look for "As" in lookup to get the table.
                item = [item for item in lookup if item.get(u"as") == look.get(u"to")][0]
                from_table = self.generate_table(item.get(u"from"), alias=look.get(u"to"))

            to_table = self.generate_table(look.get(u"from"), look.get(u"as"))

            join_tables = [from_table, to_table]
            for index, join_table in enumerate(join_tables):
                if join_table.name == table.name:
                    join_tables[index].is_root_table = True

            joins.append(Join(
                from_table=from_table,
                to_table=to_table,
                from_field=self.generate_field(from_table, field_name=look[u"localField"]),
                to_field=self.generate_field(to_table, field_name=look[u"foreignField"]),
                as_alias=look.get(u"as", look[u"from"]),
                type=look.get(u"type", u"simple")
            ))

        return joins

    def _decode_joins(self, statement, lookup):
        join_tables = []
        if lookup:
            statement.joins = self.decode_lookup(statement.table, lookup)

        to_replace = []
        for join in statement.joins:
            table_aliases = [table[1].alias for table in join_tables] + [statement.table.name]
            join_tables += [
                (join.as_alias, table) for table in [
                    join.from_table, join.to_table
                ] if table.alias not in table_aliases
            ]

            if join.type == u"simple":
                to_replace += [(join.from_field.alias, join.to_field.alias)]

        for prefix, table in join_tables:
            statement.fields += self.get_available_fields(table, prefix)


        for (alias_to_replace, replacement_alias) in to_replace:

            index_to_replace = None
            index_to_delete = None

            for index, field in reversed(list(enumerate(statement.fields))):
                if field.alias == alias_to_replace:
                    index_to_replace = index
                elif field.alias == replacement_alias:
                    index_to_delete = index
                if index_to_replace and index_to_delete:
                    break

            statement.fields[index_to_replace].alias = statement.fields[index_to_delete].alias
            del statement.fields[index_to_delete]

        return statement

    def decode_delete_many(self, table_name, query, lookup=None):
        delete_stmt = Delete()
        delete_stmt.table = self.generate_table(table_name=table_name, is_root_table=True)

        delete_stmt.fields = self.get_available_fields(delete_stmt.table)

        delete_stmt = self._decode_joins(delete_stmt, lookup)

        delete_stmt.filters = self.decode_query(query, fields=delete_stmt.fields)
        if len(delete_stmt.filters.filters) == 0:
            raise BadRequest(u"You need to supply at least one filter.")

        return delete_stmt

    def json_to_one_level(self, obj, parent=None):
        """
        Take a dict and update all the path to be on one level.
        Arguments:
            output (dict): The dict to proceed.
            parent (unicode): The parent key. Used only with recursion.
        Return:
            dict: The updated obj.
        """

        output = {}
        for key, value in obj.items():
            if isinstance(value, dict):
                if parent is None:
                    output.update(self.json_to_one_level(value, key))
                else:
                    output.update(self.json_to_one_level(value, u".".join([parent, key])))
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    item = {
                        unicode(index): item
                    }
                    if parent is None:
                        output.update(self.json_to_one_level(item, u".".join([key])))
                    else:
                        output.update(self.json_to_one_level(item, u".".join([parent, key])))
            else:
                if parent is not None:
                    output[u".".join([parent, key])] = value
                else:
                    output[key] = value
        return output
