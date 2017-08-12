# coding: utf-8
"""
Various Class and types to describe requests API
"""

# Statement = Set of word with make a request.


class Statement(object):
    pass

class StatementExpression(object):
    pass

class SelectStatement(Statement):
    def __init__(self):
        self.fields = []
        self.table = None
        self.where = []
        self.limit = None
        self.offset = None
        self.sorts = []
        self.lookup = []

class Lookup(StatementExpression):
    def __init__(self, from_collection, local_field, foreign_field, as_field):
        self.from_collection = from_collection
        self.local_field = local_field
        self.foreign_field = foreign_field
        self.as_field = as_field

class Limit(StatementExpression):
    def __init__(self, value):
        self.value = value

class Offset(StatementExpression):
    def __init__(self, value):
        self.value = value


class Value(StatementExpression):
    def __init__(self, value):
        self.value = value


class Column(object):
    """
    Column representation for a Table / Collection.
    """

    def __init__(self, name, typ, required, key, default, extra):
        self.name = name
        self.type = typ
        self.required = required
        self.key = key
        self.default = default
        self.extra = extra

class Field(object):

    def __init__(self, column, alias=None):
        self.column = column
        self.alias = alias

class Table(StatementExpression):
    def __init__(self, value):
        self.value = value


class Operator(StatementExpression):
    """
    An operator.
    """

    def __init__(self, value):
        """
        Args:
            value: The value of the object.
        """
        if value not in [u"=", u"!=", u">", u">=", u"<=", u"and", u"or"]:
            raise ValueError(u"Unknown operator '{}'.".format(value))

        self.value = value

class Sort(StatementExpression):
    def __init__(self, field, direction):
        """
        Args:
            value: The value of the object.
        """
        if direction not in [1, -1]:
            raise ValueError(u"Unknown sort '{}'.".format(direction))

        self.value =[field, direction]

