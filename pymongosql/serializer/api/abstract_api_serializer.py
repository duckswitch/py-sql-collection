# coding: utf-8
"""
This file contains AbstractSerializer class.
"""
from abc import ABCMeta, abstractmethod


class AbstractApiSerializer(object):
    """
    Defines how an API serializer should work.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def decode_limit(self, limit, statement):
        pass

    @abstractmethod
    def decode_query(self, collection, filters, projection, columns):
        """
        Parse a filters from the API.
        """
        pass

    @abstractmethod
    def decode_where(self, filters, columns, parent=None):
        """
        Parse a filters from the API.
        """
        pass