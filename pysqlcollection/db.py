# coding: utf-8
"""
This file contains DB class.
"""

from .collection import Collection


class DB(object):
    """
    Serialize MySQL requests.
    """

    def __init__(self, api_serializer, sql_serializer, connection):
        """
        Args:
            api_serializer (object): The serializer from api to neutral language.
            sql_serializer (object): The serializer from neutral language to SQL.
            connection (AbstractConnection): The object which interacts with Database.
        """
        self._api_serialize = api_serializer
        self._sql_serializer = sql_serializer
        self._connection = connection
        self.discover_tables()

    def discover_tables(self):
        tables, _ = self._connection.execute(*self._sql_serializer.get_tables())
        
        for table in tables:
            setattr(
                self,
                table[0],
                Collection(
                    self._api_serialize,
                    self._sql_serializer,
                    self._connection,
                    self._connection._database,
                    table[0]
                )
            )

