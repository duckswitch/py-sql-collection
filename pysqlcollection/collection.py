# coding: utf-8
"""
This file contains Collection class.
"""

from .cursor import Cursor
from .serializer.api_type import InsertResultOne, UpdateResult, DeleteResult


class Collection(object):
    """
    Serialize Collection requests.
    """

    def __init__(self, api_serializer, sql_serializer, connection, database_name, table_name):
        """
        Args:
            api_serializer  : The serializer from api to neutral language.
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
    
    def __getattr__(self, name):
        if name not in self.__dict__:
            self.discover_columns()
            
    def discover_columns(self, table_name=None):
        """
        Load the columns of the table.
        Args:
            table_name (unicode): The name of the table we want the columns.
        """
        if table_name not in self._api_serializer.table_columns:
            table_name = table_name or self.table_name
            result, _ = self._connection.execute(
                *self._sql_serializer.get_table_columns(table_name)
            )
            self._api_serializer.table_columns[table_name] = [
                self._sql_serializer.interpret_db_column(db_column)
                for db_column in result
            ]

    def _auto_lookup(self, table_name=None, prefix=None, deep=0, max_deep=2):
        """
        Autolookup method. Construct a list of lookup.
        Args:
            table_name (unicode): The name of the concerned table.
            prefix (unicode): A prefix to add to the lookup when we go deep in relations.
            deep (int): How deep we are in the lookup.
            max_deep (int): Recursive call count before we stop digging.
        """
        lookup = []
        if deep < max_deep:

            table_name = table_name or self.table_name
            prefix = prefix.split(u".") if prefix else []

            if deep > 0:
                prefix = [table_name]

            relations, _ = self._connection.execute(
                *self._sql_serializer.get_relations(self._database_name, table_name)
            )

            for relation in relations:
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
                lookup += self._auto_lookup(
                    item.get(u"from"), u".".join(prefix), deep=deep+1, max_deep=max_deep
                )

        return lookup

    def _proceed_lookup(self, lookup=None, auto_lookup=0):
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

        return lookup

    def find(self, query=None, projection=None, lookup=None, auto_lookup=0):
        """
        Does a find query on the collection.
        Args:
            query (dict): The mongo like query to execute.
            projection (dict): The projection parameter determines which fields are returned
                in the matching documents.
            lookup (list of dict): The lookup to apply during this query.
            auto_lookup (int): If we don't know what lookup we want, we let the lib to look
                them for us. This can have consequences on optimization as it constructs
                joins. Be careful.
        """

        lookup = self._proceed_lookup(lookup, auto_lookup)
        select = self._api_serializer.decode_find(self.table_name, query, projection, lookup)

        return Cursor(self._sql_serializer, self._api_serializer, self._connection, select)

    def insert_one(self, document):
        """
        Inserts a document in the collection.
        Args:
            document (dict): The representation of the item to insert.
        Return:
            (InsertResultOne): The representation of a insertion.
        """

        insert = self._api_serializer.decode_insert_one(self.table_name, document)
        query, values = self._sql_serializer.encode_insert(insert)

        return InsertResultOne(
            inserted_id=self._connection.execute(query, values, return_lastrowid=True)
        )

    def update_many(self, query, update, options, lookup=None, auto_lookup=0):
        """
        Updates many documents regarding the query / update passed in parameter.
        Args:
            query (dict): The query defining which documents we want to update.
            update (dict): The update operation to perform.
            options (dict): Option of the request.
            lookup (list of dict): The lookup to apply during this query.
            auto_lookup (int): If we don't know what lookup we want, we let the lib to look
                them for us. This can have consequences on optimization as it constructs
                joins. Be careful.
        """
        lookup = self._proceed_lookup(lookup, auto_lookup)
        update = self._api_serializer.decode_update_many(
            self.table_name, query, update, options, lookup
        )
        query, values = self._sql_serializer.encode_update_many(update)
        updated_row_id = self._connection.execute(query, values, return_rowcount=True)
        return UpdateResult(matched_count=updated_row_id, modified_count=updated_row_id)

    def delete_many(self, query, lookup=None, auto_lookup=0):
        """
        Deletes all document matching the filter.
        Args:
            query (dict): Where we want to delete documents.
            lookup (list of dict): The lookup to apply during this query.
            auto_lookup (int): If we don't know what lookup we want, we let the lib to look
                them for us. This can have consequences on optimization as it constructs
                joins. Be careful.
        """
        lookup = self._proceed_lookup(lookup, auto_lookup)
        delete = self._api_serializer.decode_delete_many(
            self.table_name, query=query, lookup=lookup
        )
        query, values = self._sql_serializer.encode_delete_many(delete)
        return DeleteResult(
            deleted_count=self._connection.execute(query, values, return_rowcount=True)
        )
