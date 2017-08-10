# coding: utf-8
"""
This file contains Cursor class
"""

from serializer.api.api_serializer_types import Limit, Offset


class Cursor(object):
    """
    Handle interactions with result set from DB.
    """

    def __init__(self, sql_serializer, connection, table, operation, statements):
        self._table = table
        self._sql_serializer = sql_serializer
        self.chain = [operation]
        self.statements = [statements]
        self._connection = connection
        self._sql_cursor = None
        self._batch_size = 20

    def limit(self, limit):
        """
        Add limitation to result set.
        Args:
            limit (int): Max number of result.
        """
        self.statements.append([Limit(limit)])
        self.chain.append(u"limit")
        return self

    def serialize(self):
        for index, operation in enumerate(self.chain):
            if operation == u"find":
                query, values = self._sql_serializer.query(table=self._table, query=self.statements[index])
                self._sql_cursor = self._connection.execute(query, values)
    
    def to_json(self, row, description):
        output = {}
        for index, value in enumerate(row):
            
            value = float(value) if isinstance(value, long) else value
            output[description[index][0]] = value
        return output

    def batch_size(self, batch_size):
        self._batch_size = batch_size

    def __iter__(self):
        if not self._sql_cursor:
            self.serialize()

        first = True
        fetched = []
        while first or len(fetched) > 0:
            fetched = self._sql_cursor.fetchmany(self._batch_size)
            for item in fetched:
                yield self.to_json(item, self._sql_cursor.description)
            first = False