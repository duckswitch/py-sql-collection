# coding: utf-8
"""
Various Class and types to describe requests API
"""


class ApiLanguageObject():
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
    pass
