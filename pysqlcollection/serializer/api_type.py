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

    def __init__(self, table, column, alias=None, display=True):
        """
        The representation of a field.
        Args:
            table (Table): The table it comes from.
            column (Column): The column it displays.
            alias (unicode): An optional alias to use.
            display (boolean): When it is relevant, do we display it or not ?
        """
        self.table = table
        self.column = column
        self.alias = alias
        self.display = display

    def __str__(self):
        return u"Field({}.{} AS {})".format(self.table.name, self.column.name, self.alias)





class Value(object):

    def __init__(self, column, value):
        self.column = column
        self.value = value


class Insert(object):

    def __init__(self, fields=None, table=None, values=None):
        self.table = table
        self.fields = fields or []
        self.values = values or []


class Set(object):

    def __init__(self, field, value):
        self.field = field
        self.value = value


class Join(object):

    def __init__(self, from_table, to_table, from_field, to_field, as_alias, type=u"simple"):
        """
        Constructs the representation of a Join.
        Args:
            from_table (Table): We join from this table.
            to_table (Table): We join to this table.
            from_field (Field): We join from this field.
            to_field (Field): We join to this field.
            as_alias (unicode): Alias to refer at field coming from joined table.
            type (unicode): Can be simple or multiple.
        """
        self.from_table = from_table
        self.to_table = to_table
        self.from_field = from_field
        self.to_field = to_field
        self.as_alias = as_alias
        self.type = type

    def __str__(self):
        return u"Join({} `{}` TO {} `{}` ON `{}`.{} = `{}`.{})".format(
            self.from_table.name,
            self.from_table.alias,
            self.to_table.name,
            self.to_table.alias,
            self.from_table.alias,
            self.from_field.column.name,
            self.to_table.alias,
            self.to_field.column.name
        )

class Or(object):
    def __init__(self, filters):
        self.filters = filters

class And(object):
    def __init__(self, filters):
        self.filters = filters

class Filter(object):

    def __init__(self, field, operator, value):
        self.field = field
        self.operator = operator
        self.value = value

class Operator(object):
    def __init__(self, value):
        self.value = value

class InsertResultOne(object):

    def __init__(self, inserted_id):
        self.inserted_id = inserted_id

class UpdateResult(object):

    def __init__(self, matched_count, modified_count):
        self.matched_count = matched_count
        self.modified_count = modified_count

class DeleteResult(object):

    def __init__(self, deleted_count):
        self.deleted_count = deleted_count

class Update(object):
    def __init__(self, table=None, fields=None, sets=None, joins=None, filters=None):
        self.table = table
        self.fields = fields or []
        self.sets = sets or []
        self.joins = joins or []
        self.filters = filters or None

class Delete(object):
    def __init__(self, table=None, fields=None, joins=None, filters=None):
        self.table = table
        self.fields = fields or []
        self.joins = joins or []
        self.filters = filters or None

class Sort():
    """
    This object wraps a Sort definition for the API.
    """
    def __init__(self, field, direction):
        self.field = field
        self.direction = direction

class Select(object):
    """
    Object which wraps a Select request for the API.
    """
    def __init__(self, fields=None, table=None, joins=None, limit=None, offset=0, filters=None, sorts=None, aggregation=None):
        self.fields = fields or []
        self.table = table
        self.joins = joins or []
        self.filters = filters
        self.limit = limit or 100
        self.offset = offset or 0
        self.sorts = sorts or []
        self.aggregation = None
