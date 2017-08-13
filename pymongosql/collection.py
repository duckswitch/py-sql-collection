# coding: utf-8
"""
This file contains Collection class.
"""

import json
from .cursor import Cursor
from .serializer.api_type import InsertResultOne


class Collection(object):
    """
    Serialize Collection requests.
    """

    def __init__(self, api_serializer, sql_serializer, connection, database_name, table_name):
        """
        Args:
            api_serializer : The serializer from api to neutral language.
            sql_serializer : The serializer to translate to SQL requests.
            connection : The object which interacts with Database.
            database_name (unicode): The name of the database where the table comes from.
            table_name (unicode): The name of the table associated.
        """
        self._api_serializer = api_serializer
        self._sql_serializer = sql_serializer
        self._connection = connection
        self._database_name = database_name
        self.table_name = table_name
        # Discover table config.


    def discover_columns(self, table_name=None):
        """
        Load the columns of the table.
        """
        if table_name not in self._api_serializer.table_columns:
            table_name = table_name or self.table_name
            result, _ = self._connection.execute(*self._sql_serializer.get_table_columns(table_name))
            self._api_serializer.table_columns[table_name] = [
                self._sql_serializer.interpret_db_column(db_column)
                for db_column in result
            ]

    def _auto_lookup(self, table_name=None, prefix=None, deep=0, max_deep=2):

        lookup = []
        if deep < max_deep:

            table_name = table_name or self.table_name
            prefix = prefix.split(u".") if prefix else []

            if deep > 0:
                prefix = [table_name]

            relations, _ = self._connection.execute(*self._sql_serializer.get_relations(self._database_name, table_name))

            for relation in relations:
                print(prefix)
                item = {
                    u"from": relation[2],
                    u"localField": relation[1],
                    u"foreignField": relation[3],
                    u"as": u".".join(prefix + [relation[2]])
                }
                if deep > 0:
                    item[u"to"] = relation[0]

                lookup.append(item)

            for item in lookup:
                lookup += self._auto_lookup(item.get(u"from"), u".".join(prefix), deep=deep+1, max_deep=max_deep)

        return lookup

    def find(self, query=None, projection=None, lookup=None, auto_lookup=0):
        """
        Do a find query on the collection.
        Args:
            query (dict): The mongo like query to execute.
            projection (dict): The projection parameter determines which fields are returned
                in the matching documents.
        """

        if lookup is None:
            lookup = self._auto_lookup(max_deep=auto_lookup)


        potential_tables = [self.table_name]
        if lookup is not None:
            potential_tables += [
                item[u"from"] for item in lookup
            ] + [
                item[u"to"] for item in lookup if u"to" in item
            ]

        for table in potential_tables:
            self.discover_columns(table)



        select = self._api_serializer.decode_find(self.table_name, query, projection, lookup)

        return Cursor(self._sql_serializer, self._api_serializer, self._connection, select)

    def insert_one(self, document):

        insert = self._api_serializer.decode_insert_one(self.table_name, document)
        query, values = self._sql_serializer.encode_insert(insert)

        return InsertResultOne(inserted_id=self._connection.execute(query, values, return_lastrowid=True))