# coding: utf-8
"""
This file contains Cursor class
"""

from serializer.api.api_serializer_types import Limit, Offset
from serializer.api.api_serializer_types import SelectStatement

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
        if isinstance(self.statement, SelectStatement):
            self.statement = self._api_serializer.decode_limit(self.statement, limit)

        return self

    def sort(self, key_or_list, direction=None):
        if isinstance(self.statement, SelectStatement):
            self.statement = self._api_serializer.decode_sort(self.statement, key_or_list, direction)
        return self

    def skip(self, skip):
        """
        Skip items.
        Args:
            limit (int): Max number of result.
        """
        if isinstance(self.statement, SelectStatement):
            self.statement = self._api_serializer.decode_skip(self.statement, skip)

        return self

    def get_(self, operation):
        try:
            index = self.chain.index(operation)
        except ValueError:
            return None

    def serialize(self):
        if isinstance(self.statement, SelectStatement):
            query, values = self._sql_serializer.query(self.statement)
            rows, description = self._connection.execute(query, values)
            self._items = [self.to_json(row, description) for row in rows]

        self._executed = True

    def to_json(self, row, description):
        output = {}
        for index, value in enumerate(row):
            
            value = float(value) if isinstance(value, long) else value
            output[description[index][0]] = value
        return output

    def batch_size(self, batch_size):
        self._batch_size = batch_size

    def __iter__(self):
        if not self._executed:
            self.serialize()

        for item in self._items:
            yield item