# coding: utf-8
"""
This file contains tests for MySQLSerializer class.
"""

from pytest import fixture

from pysqlcollection.serializer.mysql_serializer import MySQLSerializer


@fixture(scope=u"function")
def mysql_serializer():
    """
    Initiate object.
    """
    return MySQLSerializer()

def test_get_tables(mysql_serializer):
    """
    Test get table method.
    """
    query, values = mysql_serializer.get_tables()
    assert query == u"SHOW TABLES"
    assert values == []

def test_get_table_columns(mysql_serializer):
    """
    Test get table method.
    """
    query, values = mysql_serializer.get_table_columns(u"project")
    assert query == u"DESCRIBE project"
    assert values == []

def test_interpret_db_column(mysql_serializer):
    """
    Test different cases of column parsing.
    """
    column = mysql_serializer.interpret_db_column(
        (u"id", u"int(11)", u"NO", u"PRI", None, u"auto_increment")
    )
    assert column.name == u"id"
    assert column.type == u"number"
    assert column.required is True
    assert column.key == u"pri"
    assert column.default is None
    assert column.extra == u"auto_increment"

    column = mysql_serializer.interpret_db_column(
        (u"id", u"varchar(50)", u"NO", u"", None, u"")
    )
    assert column.name == u"id"
    assert column.type == u"string"
    assert column.required is True
    assert column.key == u""
    assert column.default is None
    assert column.extra == u""

    column = mysql_serializer.interpret_db_column(
        (u"started_at", u"datetime", u"YES", u"", None, u"")
    )
    assert column.name == u"started_at"
    assert column.type == u"timestamp"
    assert column.required is False
    assert column.key == u""
    assert column.default is None
    assert column.extra == u""
