# coding: utf-8
"""
This file contains AbstractConnection class.
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
            host=None,
            unix_socket=None,
            database=None,
    ):

        self._host = host
        self._unix_socket = unix_socket
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
    def execute(self, query, values, return_lastrowid=False, return_rowcount=False):
        """
        Execute a query.
        Args:
            (unicode): The query.
            (list): The values to inject in the query.
        """
        pass
