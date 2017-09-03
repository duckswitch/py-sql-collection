# coding: utf-8
"""
This file contains Cursor class
"""

import json
import calendar
from datetime import datetime
from .serializer.api_type import (
    Select,
    Insert
)
from .utils import json_get, json_set


class Cursor(object):
    """
    Handle interactions with result set from DB.
    """

    def __init__(self, sql_serializer, api_serializer, connection, statement, lookup=None):
        self._sql_serializer = sql_serializer
        self._api_serializer = api_serializer
        self.statement = statement
        self._lookup = lookup or []
        self._connection = connection
        self._executed = False
        self._items = []
        self.inserted_id = None

    def limit(self, limit):
        """
        Add limitation to result set.
        Args:
            limit (int): Max number of result.
        """
        if isinstance(self.statement, Select):
            self.statement = self._api_serializer.decode_limit(self.statement, limit)

        return self

    def count(self, with_limit_and_skip=False):
        if isinstance(self.statement, Select):
            query, values = self._sql_serializer.encode_select_count(self.statement, with_limit_and_skip)
            rows, _ = self._connection.execute(query, values)
            return int(rows[0][0])

    def sort(self, key_or_list, direction=None):
        """
        Applies a sort on the cursor.
        Args:
            key_or_list (unicode or list of tuple): Field to sort or sort list for many fields.
            direction (int): Direction if only one key supplied to the sort.
        Return:
            (Cursor): The updated cursor.
        """
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

    # def _old_deduplicate(self, items, prim_key, duplicated, foreign_key):
    #     items = list(items)
    #     last_key = None
    #     last_duplicated_key = None
    #     output = []
    #     first = True
    #     acc = []
    #     found_relations = []
    #
    #     for index, item in reversed(list(enumerate(items))):
    #         key = json_get(item, prim_key)
    #         duplicated_key = json_get(item,  u"{}.{}".format(duplicated, foreign_key))
    #         if key != last_key and not first or index == 0:
    #             if index == 0:
    #                 acc.insert(0, json_get(item, duplicated))
    #
    #             output.insert(0, acc)
    #             acc = []
    #             last_duplicated_key = None
    #             if index == 0:
    #                 break
    #
    #         if foreign_key != last_duplicated_key:
    #             acc.insert(0, json_get(item, duplicated))
    #
    #         first = False
    #
    #         last_key = key
    #         last_duplicated_key = duplicated_key
    #
    #         del items[-1]
    #
    #     return output

    def _deduplicate(self, items, prim_key, duplicated, foreign_primary_key):
        prim_key_path = prim_key
        foreign_key_path = u"{}.{}".format(duplicated, foreign_primary_key)
        relations = {}

        for index, item in reversed(list(enumerate(items))):
            prim_key = json_get(item, prim_key_path)
            duplicated_key = json_get(item, foreign_key_path)
            relation_key = u"{}:{}".format(prim_key, duplicated_key)

            if prim_key not in relations:
                relations[prim_key] = {}

            if relation_key not in relations[prim_key] and duplicated_key:
                relations[prim_key][relation_key] = json_get(item, duplicated)

            del items[-1]

        output = []
        for item_key in relations:
            foreign_list = []
            for foreign_primary_key in relations[item_key]:
                foreign_list.append(relations[item_key][foreign_primary_key])
            output.append(foreign_list)
        return output

    def dedup_ids(self, items, key):

        last_id = None
        for index, item in reversed(list(enumerate(items))):
            if item[key] == last_id:
                del items[index]
            last_id = item[key]
        return items

    def deduplication(self, items):

        lookup_to_deduplicate = [look for look in self._lookup if look.get(u"type") == u"multiple"]
        items = sorted(items, key=lambda item: item.get(u"id"))
        dedup_items = self.dedup_ids(list(items), u"id")

        for look in lookup_to_deduplicate:

            foreign_items = self._deduplicate(
                list(items),
                look.get(u"localField"),
                look.get(u"as"),
                look.get(u"foreignPrimaryKey", u"id")
            )
            for index, foreign_item in enumerate(foreign_items):

                json_set(dedup_items[index], look.get(u"as"), foreign_items[index])


        return dedup_items

    def serialize(self):
        if isinstance(self.statement, Select):
            query, values = self._sql_serializer.encode_select(self.statement)
            rows, description = self._connection.execute(query, values)
            self._items = self._items = [self.to_json(row, description) for row in rows] # self.deduplication(items=[self.to_json(row, description) for row in rows])
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
