# coding: utf-8
"""
Various Class and types to describe requests API
"""


class ApiLanguageObject(object):
    """
    Describe an API object.
    """
    def __init__(self, value):
        """
        Args:
            value: The value of the object.
        """
        self.value = value

    def __str__(self):
        """
        Called when printed.
        """
        return u"{}({})".format(self.__class__.__name__, self.value)


class Value(ApiLanguageObject):
    """
    A parameter given by user.
    """
    pass


class Field(ApiLanguageObject):
    """
    A field.
    """
    pass


class Operator(ApiLanguageObject):
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

        ApiLanguageObject.__init__(self, value=value)

class Limit(ApiLanguageObject):
    """
    A field.
    """
    pass

class Offset(ApiLanguageObject):
    """
    A field.
    """
    pass
