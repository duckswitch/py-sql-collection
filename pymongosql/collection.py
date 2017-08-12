# coding: utf-8
"""
This file contains Collection class.
"""

from .cursor import Cursor

class Collection(object):
    """
    Serialize Collection requests.
    """

    def __init__(self, api_serializer, sql_serializer, connection, table):
        """
        Args:
            api_serializer : The serializer from api to neutral language.
            sql_serializer : The serializer to translate to SQL requests.
            connection : The object which interacts with Database.
            table (unicode): The name of the table associated.
        """
        self._api_serializer = api_serializer
        self._sql_serializer = sql_serializer
        self._connection = connection
        self.table = table
        self._columns = None

    def create_cursor(self, statement):
        return Cursor(self._sql_serializer, self._api_serializer, self._connection, statement)

    @property
    def columns(self):
        """
        Load the columns of the table.
        """
        if not self._columns:
            result, _ = self._connection.execute(*self._sql_serializer.get_table_columns(self.table))
            self._columns = [
                self._sql_serializer.interpret_db_column(db_column)
                for db_column in result
            ]
        return self._columns

    def find(self, query=None, projection=None):
        """
        Do a find query on the collection.
        Args:
            query (dict): The mongo like query to execute.
            projection (dict): The projection parameter determines which fields are returned
                in the matching documents.
        """
        statement = self._api_serializer.decode_query(self.table, self.columns, query, projection)
        return self.create_cursor(statement)
        # sql_query, values = self._sql_serializer.query(table=self.table, query=decoded_query)
        # return self._connection.execute(sql_query, values)