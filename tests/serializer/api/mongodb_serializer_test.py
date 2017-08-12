# coding: utf-8
"""
This file contains tests for MySQLSerializer class.
"""

from pytest import fixture
from pymongosql.serializer.api.mongodb_serializer import MongodbSerializer
from pymongosql.serializer.api.api_serializer_types import (
    Value,
    Column,
    Operator
)

@fixture(scope=u"function")
def columns():
    return [
        Column(
            name=u"id",
            typ=u"number",
            required=True,
            key=u"",
            default=None,
            extra=u""

        ),
        Column(
            name=u"name",
            typ=u"string",
            required=True,
            key=u"",
            default=None,
            extra=u""
        ),
        Column(
            name=u"hours",
            typ=u"number",
            required=False,
            key=u"",
            default=None,
            extra=u""
        )
    ]

@fixture(scope=u"function")
def mongodb_serializer():
    """
    Initiate object.
    """
    return MongodbSerializer()


def test_decode_where_basic(mongodb_serializer, columns):
    """
    Test if filters are working.
    """

    where = mongodb_serializer.decode_where({
        u"name": u"kevin"
    }, columns)
    assert isinstance(where[0], Column)
    assert where[0].name == u"name"
    assert isinstance(where[1], Operator)
    assert where[1].value == u"="
    assert isinstance(where[2], Value)
    assert where[2].value == u"kevin"

def test_decode_where_with_operators(mongodb_serializer, columns):
    """
    Test if filters are working.
    """
    where = mongodb_serializer.decode_where({
        u"hours": {
            u"$gt": 5,
            u"$lte": 6
        }
    }, columns)

    assert isinstance(where[0], Column)
    assert where[0].name == u"hours"
    assert isinstance(where[1], Operator)
    assert where[1].value == u"<="
    assert isinstance(where[2], Value)
    assert where[2].value == 6

    assert isinstance(where[3], Column)
    assert where[3].name == u"hours"
    assert isinstance(where[4], Operator)
    assert where[4].value == u">"
    assert isinstance(where[5], Value)
    assert where[5].value == 5



