# coding: utf-8
"""
This file contain Collection class.
"""


class Collection(object):
    """
    Serialize Collection requests.
    """

    def __init__(self, serializer, connection):
        """
        Args:
            serializer (object): The serializer to translate to SQL requests.
            connection (object): The object which interacts with Database.
        """
        self._serializer = serializer
        self._connection = connection

    def find(query, projection):
        return []