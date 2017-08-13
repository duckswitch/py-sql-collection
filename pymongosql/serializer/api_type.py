# coding: utf-8
"""
Various Class and types to describe requests API
"""


class Table(object):
    """
    Table representation.
    """
    def __init__(self, name, alias=None, columns=None, is_root_table=False):
        """
        Constructs representation of a table.
        Args:
            name (unicode): The table name.
            alias (unicode): An optional alias for the SQL query.
            columns (list of Column)): The columns belonging to the table.
            is_root_table (bool): Is the root table (for selects) or not.
        """
        self.name = name
        self.alias = alias
        self.columns = columns or []
        self.is_root_table = is_root_table


class Column(object):
    """
    Column representation for a Table / Collection.
    """

    def __init__(self, name, typ, required, key, default, extra):
        """
        Constructs representation of a table column.
        Args:
            table (Table): The table the column belongs to.
            name (unicode): The name of the column.
            typ (unicode): The type of the column.
            required (bool): Is the columns required or not.
            key (unicode): Kind of key (primary, multiple, ...).
            default (unicode): Default value.
            extra (unicode): Other rules.
        """
        self.name = name
        self.type = typ
        self.required = required
        self.key = key
        self.default = default
        self.extra = extra


class Field(object):

    def __init__(self, table, column, alias=None):
        """
        The representation of a field.
        Args:
            table (Table): The table it comes from.
            column (Column): The column it displays.
            alias (unicode): An optional alias to use.
        """
        self.table = table
        self.column = column
        self.alias = alias

    def __str__(self):
        return u"Field({}.{} AS {})".format(self.table.name, self.column.name, self.alias)


class Select(object):

    def __init__(self, fields=None, table=None, joins=None, limit=None, offset=0):
        self.fields = fields or []
        self.table = table
        self.joins = joins or []
        self.limit = limit or 100
        self.offset = offset or 0


class Value(object):

    def __init__(self, column, value):
        self.column = column
        self.value = value


class Insert(object):

    def __init__(self, fields=None, table=None, values=None):
        self.table = table
        self.fields = fields or []
        self.values = values or []


class Join(object):

    def __init__(self, from_table, to_table, from_field, to_field, as_alias):
        """
        Constructs the representation of a Join.
        Args:
            from_table (Table): We join from this table.
            to_table (Table): We join to this table.
            from_field (Field): We join from this field.
            to_field (Field): We join to this field.
            as_alias (unicode): Alias to refer at field coming from joined table.
        """
        self.from_table = from_table
        self.to_table = to_table
        self.from_field = from_field
        self.to_field = to_field
        self.as_alias = as_alias

    def __str__(self):
        return u"Join({} {} TO {} {} ON {}.{} = {}.{})".format(
            self.from_table.name,
            self.from_table.alias,
            self.to_table.name,
            self.to_table.alias,
            self.from_table.name,
            self.from_field.column.name,
            self.to_table.name,
            self.to_field.column.name
        )



class InsertResultOne(object):

    def __init__(self, inserted_id):
        self.inserted_id = inserted_id