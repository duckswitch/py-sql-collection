# coding: utf-8
"""
This file contains tests for MySQLSerializer class.
"""

from pytest import fixture
from pymongosql.serializer.api.mongodb_serializer import MongodbSerializer
from pymongosql.serializer.api.api_serializer_types import (
    Value,
    Field,
    Operator
)


@fixture(scope=u"function")
def mongodb_serializer():
    """
    Initiate object.
    """
    return MongodbSerializer()


def test_decode_query_basic(mongodb_serializer):
    """
    Test if filters are working.
    """
    where = mongodb_serializer.decode_query({
        u"name": u"kevin"
    })

    for index, to_check in enumerate([
            (Field, u"name"), (Operator, u"=", (Value, u"kevin"))
        ]):

        assert isinstance(where[index], to_check[0])
        assert where[index].value == to_check[1]


def test_decode_query_with_operators(mongodb_serializer):
    """
    Test if filters are working.
    """
    where = mongodb_serializer.decode_query({
        u"hours": {
            u"$gt": 5,
            u"$lte": 6
        }
    })

    check_list = [
        (Field, u"hours"),
        (Operator, u"<="),
        (Value, 6),
        (Field, u"hours"),
        (Operator, u">"),
        (Value, 5)
    ]
    for index, to_check in enumerate(check_list):
        assert isinstance(where[index], to_check[0])
        assert where[index].value == to_check[1]

