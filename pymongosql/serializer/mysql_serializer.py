# coding: utf-8
"""
This file contain MySQLSerializer class.
"""

from .abstract_serializer import AbstractSerializer

class MySQLSerializer(AbstractSerializer):
    """
    Serialize MySQL requests.
    """

    def select(self, table, columns, joins, where, order, order_by, limit, offset):
        """
        Generate a SELECT request.
        Args:
            table (unicode): The table name.
            columns (list): A list of columns to select.
            joins (list): A list of joins statements.
            where (list): A list of filtering statements.
            order (list): The fields to order by.
            order_by (list): The sort kind for each field from order.
            limit (int): Max item count returned.
            offset (int): The page offset.
        Returns:
            (unicode, list): A query and values to inject in it.
        """
        return "SELECT * FROM project", []

    def get_tables(self):
        """
        Query to get all tables from the database.
        Returns:
            (unicode, list): A query and values to inject in it.
        """
        return "SHOW TABLES", []
