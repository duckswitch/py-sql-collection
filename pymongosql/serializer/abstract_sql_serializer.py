# coding: utf-8
"""
This file contains AbstractSerializer class.
"""
from abc import ABCMeta, abstractmethod

class AbstractSQLSerializer(object):
    """
    Defines how a Parser should be.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def encode_select(self, select):
        pass

    @abstractmethod
    def get_tables(self):
        """
        Query to get all tables from the database.
        Returns:
            (unicode, list): A query and values to inject in it.
        """
        pass

    @abstractmethod
    def get_table_columns(self, table):
        """
        Query to get all tables from the database.
        Args:
            table (unicode): The name of the table.
        Returns:
            (unicode, list): A query and values to inject in it.
        """
        pass

    @abstractmethod
    def interpret_db_column(self, row):
        """
        Receive the result from Database concerning a column, returns
        something the DB class could understand.
        Args:
            row (tuple): A row from a result set describing the column.
        Returns:
            (Column): A column object.
        """
        pass
