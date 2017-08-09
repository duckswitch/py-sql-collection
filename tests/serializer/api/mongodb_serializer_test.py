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

def test_parse_query_basic(mongodb_serializer):
    """
    Test if filters are working.
    """
    where = mongodb_serializer.parse_query({
        u"name": u"kevin"
    })

    for index, to_check in enumerate([
            (Field, u"name"), (Operator, u"=", (Value, u"kevin"))
        ]):

        assert isinstance(where[index], to_check[0])
        assert where[index].value == to_check[1]
