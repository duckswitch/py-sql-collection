# coding: utf-8
"""
This file contains AbstractSerializer class.
"""

from pysqlcollection.serializer.api_type import (
    Select,
    Insert,
    Field,
    Table,
    Join,
    Update,
    Delete,
    Value,
    Filter,
    Operator,
    Set,
    Or,
    And,
    Sort
)
from .api_exception import (
        WrongParameter
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

        if query:
            select.filters = self.decode_query(query, fields=select.fields)

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
                            value=value
                        ))

                elif key in self._OPERATORS:

                    field = fields[field_names.index(parent)]
                    operator = Operator(self._OPERATORS[key])
                    filters.append(Filter(
                        field,
                        operator,
                        value=value
                    ))
      
        return join_operator(filters)

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

    def decode_delete_many(self, table_name, query, lookup=None):
        delete_stmt = Delete()
        delete_stmt.table = self.generate_table(table_name=table_name, is_root_table=True)
        if lookup:
            delete_stmt.joins = self.decode_lookup(delete_stmt.table, lookup)

        delete_stmt.fields = self.get_available_fields(delete_stmt.table)
        delete_stmt.filters = self.decode_query(query, fields=delete_stmt.fields)
        return delete_stmt
