# coding: utf-8
"""
This file contain DB class.
"""

from .collection import Collection

class DB(object):
    """
    Serialize MySQL requests.
    """

    def __init__(self, serializer, connection):
        """
        Args:
            serializer (object): The serializer to translate to SQL requests.
            connection (object): The object which interacts with Database.
        """
        self._serializer = serializer
        self._connection = connection
        self.discover_tables()

    def discover_tables(self):
        result, _ = self._connection.execute(*self._serializer.get_tables())
        for table in result:
            setattr(self, table[0], Collection(self._serializer, self._connection))
