# coding: utf-8
"""
Transaction context manager.
"""


class Transaction(object):
    """
    Handles transactions with Database.
    """

    def __init__(self, connection):
        """
        Constructor.
        Args:
            connection (AbstractConnection): Object which handles connection.
        """
        self.connection = connection
        self.sql_cursor = None
        self.sql_connection = None

    def begin(self):
        """
        Begin the transaction by opening connection & cursor.
        """
        self.sql_connection = self.connection.connect()
        self.sql_cursor = self.sql_connection.cursor()

    def __enter__(self):
        """
        Called when used as a context manager.
        Returns:
            (Transaction): The returned object.
        """
        self.begin()
        return self

    def __exit__(self, type, value, traceback):
        """
        Called at the end of the with.
        Args:
            type: Type returned.
            value: Value returned.
            traceback (traceback): The potential exception.
        """
        if not traceback:
            self.commit()
        else:
            self.rollback()

        self.close()

    def commit(self):
        """
        To commit requests.
        """
        self.sql_connection.commit()

    def rollback(self):
        """
        To cancel all operations done since the begin call.
        """
        self.sql_connection.rollback()

    def close(self):
        """
        To close the connection at the end of the transaction, after the commit.
        """
        self.sql_connection.close()
        self.sql_cursor.close()