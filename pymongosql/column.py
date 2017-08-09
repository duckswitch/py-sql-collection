# coding: utf-8
"""
This file contains Column class.
"""


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
    