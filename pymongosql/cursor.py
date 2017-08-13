# coding: utf-8
"""
This file contains Cursor class
"""

import calendar
from datetime import datetime
from .serializer.api_type import (
    Select
)
class Cursor(object):
    """
    Handle interactions with result set from DB.
    """

    def __init__(self, sql_serializer, api_serializer, connection, statement):
        self._sql_serializer = sql_serializer
        self._api_serializer = api_serializer
        self.statement = statement
        self._connection = connection
        self._executed = False
        self._items = []

    def limit(self, limit):
        """
        Add limitation to result set.
        Args:
            limit (int): Max number of result.
        """
        if isinstance(self.statement, Select):
            self.statement = self._api_serializer.decode_limit(self.statement, limit)

        return self

    def sort(self, key_or_list, direction=None):
        if isinstance(self.statement, Select):
            self.statement = self._api_serializer.decode_sort(self.statement, key_or_list, direction)
        return self

    def skip(self, skip):
        """
        Skip items.
        Args:
            limit (int): Max number of result.
        """
        if isinstance(self.statement, Select):
            self.statement = self._api_serializer.decode_skip(self.statement, skip)

        return self

    def get_(self, operation):
        try:
            index = self.chain.index(operation)
        except ValueError:
            return None

    def serialize(self):
        if isinstance(self.statement, Select):
            query, values = self._sql_serializer.encode_select(self.statement)
            rows, description = self._connection.execute(query, values)
            self._items = [self.to_json(row, description) for row in rows]

        self._executed = True

    def json_set(self, item, path, value):
        """
        Set the value corresponding to the path in a dict.
        Arguments:
            item (dict): The object where we want to put a field.
            path (unicode): The path separated with dots to the field.
            value: The value to set on the field.
        Return:
            (dict): The updated object.
        """
        tab = path.split(u".")
        if tab[0] not in item and len(tab) > 1:
            item[tab[0]] = {}
        if len(tab) == 1:
            item[tab[0]] = value
        else:
            item[tab[0]] = self.json_set(item[tab[0]], u".".join(tab[1:]), value)
        return item

    def to_json(self, row, description):
        output = {}
        for index, value in enumerate(row):
            if isinstance(value, long):
                value = float(value)
            elif isinstance(value, datetime):
                value =  calendar.timegm(value.utctimetuple())

            output = self.json_set(item=output, path=description[index][0], value=value)
        return output

    def batch_size(self, batch_size):
        self._batch_size = batch_size

    def __iter__(self):
        if not self._executed:
            self.serialize()

        for item in self._items:
            yield item