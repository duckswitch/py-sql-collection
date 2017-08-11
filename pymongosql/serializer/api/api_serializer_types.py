# coding: utf-8
"""
Various Class and types to describe requests API
"""

# Statement = Set of word with make a request.


class Statement(object):
    pass


class StatementExpression(object):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        """
        Called when printed.
        """
        return u"{}({})".format(self.__class__.__name__, self.value)


class SelectStatement(Statement):
    def __init__(self):
        self.fields = []
        self.table = None
        self.where = []
        self.limit = None
        self.offset = None
        self.sorts = []


class Limit(StatementExpression):
    pass




class Offset(StatementExpression):
    pass


class Value(StatementExpression):
    pass


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


class Table(StatementExpression):
    pass


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

        StatementExpression.__init__(self, value=value)


class Sort(StatementExpression):
    def __init__(self, field, direction):
        """
        Args:
            value: The value of the object.
        """
        if direction not in [1, -1]:
            raise ValueError(u"Unknown sort '{}'.".format(direction))

        StatementExpression.__init__(self, value=[field, direction])

