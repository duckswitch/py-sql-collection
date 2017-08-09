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
    def decode_query(self, filters, parent=None):
        """
        Parse a filters from the API.
        """
        pass