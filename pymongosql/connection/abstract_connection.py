# coding: utf-8
"""
This file contain AbstractConnection class.
"""
from abc import ABCMeta, abstractmethod

class AbstractConnection(object):
    """
    Defines how a ConnectionClass should be.
    """
    __metaclass__ = ABCMeta

    def __init__(
            self,
            user,
            password,
            database,
            host
    ):

        self._host = host
        self._user = user
        self._password = password
        self._database = database

    @abstractmethod
    def connect(self):
        """
        Connect to the database. Return a cursor.
        Returns
            (object): The DB Connection.
        """
        pass

    @abstractmethod
    def execute(self, query, values):
        """
        Execute a query.
        Args:
            (unicode): The query.
            (list): The values to inject in the query.
        """
        pass
