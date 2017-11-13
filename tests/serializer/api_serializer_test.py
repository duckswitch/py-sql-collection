# coding: utf-8
"""
This file contains tests for ApiSerialiser class.
"""

import pytest
import json
from pytest import fixture
from collections import OrderedDict
from pysqlcollection.serializer.api_serializer import ApiSerializer
from pysqlcollection.serializer.api_type import (
    Select,
    Column,
    Table,
    Field,
    And,
    Sort,
    Or
)
from pysqlcollection.serializer.api_exception import (
    WrongParameter
)


@fixture(scope=u"function")
def client_table_columns():
    return [
        Column(name=u"id", typ=u"number", required=True, key=u"pri", extra=u"auto_increment", default=None),
        Column(name=u"name", typ=u"string", required=True, key=u"", extra=u"", default=None)
    ]


@fixture(scope=u"function")
def project_table_columns():
    return [
        Column(
            name=u"id",
            typ=u"number",
            required=True,
            key=u"pri",
            extra=u"auto_increment",
            default=None),
        Column(name=u"name", typ=u"string", required=True, key=u"", extra=u"", default=None),
        Column(name=u"client_id", typ=u"number", required=True, key=u"mul", extra=u"", default=None)
    ]


@fixture(scope=u"function")
def dummy_table_columns(client_table_columns, project_table_columns):
    return {
        u"client": client_table_columns,
        u"project": project_table_columns
    }


@fixture(scope=u"function")
def api_serializer(dummy_table_columns):
    """
    Initiate object.
    """
    api_serializer = ApiSerializer()
    api_serializer.table_columns = dummy_table_columns
    return api_serializer


@fixture(scope=u"function")
def client_table(api_serializer):
    return Table(
        name=u"client",
        alias=u"client",
        columns=api_serializer.table_columns[u"client"],
        is_root_table=True
    )


@fixture(scope=u"function")
def dummy_select(client_table, client_table_columns):
    """
    Initiate object.
    """
    return Select(
        table=client_table,
        fields=[
            Field(
                table=client_table,
                column=column,
                alias=column.name)
            for column in client_table_columns
        ]
    )


def test_decode_limit(api_serializer, dummy_select):
    """
    Test for simple parameters.
    """
    select = api_serializer.decode_limit(dummy_select, 10)
    assert select.limit == 10

    with pytest.raises(WrongParameter) as exec_info:
        api_serializer.decode_limit(dummy_select, -12)

    assert exec_info.value.api_error_code == u"WRONG_PARAMETER"


def test_decode_skip(api_serializer, dummy_select):
    """
    Test for simple parameters.
    """
    select = api_serializer.decode_skip(dummy_select, 10)
    assert select.offset == 10

    with pytest.raises(WrongParameter) as exec_info:
        api_serializer.decode_skip(dummy_select, -12)

    assert exec_info.value.api_error_code == u"WRONG_PARAMETER"


def test_decode_sort(api_serializer, dummy_select):
    """
    Test for simple parameters.
    """
    select = api_serializer.decode_sort(dummy_select, u"name", 1)
    assert len(select.sorts) == 1
    assert select.sorts[0].field.alias == u"name"
    assert select.sorts[0].direction == 1


def test_generate_table(api_serializer):
    """
    Test simple working of generate_table method.
    """
    table = api_serializer.generate_table(
        table_name=u"client",
        alias=u"DummyAlias",
        is_root_table=True
    )
    assert isinstance(table, Table)
    assert len(table.columns) == 2
    assert table.alias == u"DummyAlias"
    assert table.is_root_table


def test_generate_field(api_serializer, client_table):
    """
    test simple working of generate_field method.
    """
    # test when table is root.
    args = [client_table, u"name", u"project.client"]
    field = api_serializer.generate_field(*args)
    assert field.alias == u"name"
    assert field.column == api_serializer.table_columns[u"client"][1]
    assert field.table == client_table

    # test alias variations
    client_table.is_root_table = False
    field = api_serializer.generate_field(*args)
    assert field.alias == u"project.client.name"

    # No prefix
    args = [client_table, u"name"]
    field = api_serializer.generate_field(*args)
    assert field.alias == u"client.name"


def test_get_available_fields(api_serializer, client_table):
    """
    Test simple working of get_available_fields.
    """
    client_id_column = api_serializer.table_columns[u"client"][0]
    fields = api_serializer.get_available_fields(
        client_table,
        prefix=u"project.client",
        to_ignore=[
            Field(
                table=client_table,
                column=client_id_column
            )
        ]
    )
    assert len(fields) == 1
    assert fields[0].column == client_table.columns[1]


@fixture(scope=u"function")
def project_fields(api_serializer):
    """
    Get fields from a table named project.
    """
    project_table = api_serializer.generate_table(u"project")
    project_table.is_root_table = True
    client_table = api_serializer.generate_table(u"client")

    return (
        api_serializer.get_available_fields(project_table) +
        api_serializer.get_available_fields(client_table, prefix=u"client")
    )


def test_decode_query_equal(api_serializer, project_fields):
    """
    Test with equal filter.
    """
    root_and = api_serializer.decode_query(
        {u"name": u"test"},
        project_fields
    )
    assert root_and.filters[0].field.column.name == u"name"
    assert root_and.filters[0].operator.value == u"="
    assert root_and. filters[0].value == u"test"


def test_decode_query_custom_op(api_serializer, project_fields):
    """
    Test with $gte filter & joined field.
    """
    # Order is important to don't break the tests.
    filt = json.loads("""
    {
        "client.id": {
            "$gte": 5,
            "$lte": 12
        }
    }
    """, object_pairs_hook=OrderedDict)
    root_and = api_serializer.decode_query(
        filt,
        project_fields
    )
    # Because it's a dict into a dict, another level is created.
    # We expect this :
    # And(
    #   And(
    #       >= 5,
    #       <= 12
    #   )
    # )
    assert len(root_and.filters) == 1 and isinstance(root_and.filters[0], And)
    client_id_and = root_and.filters[0]
    gte = client_id_and.filters[0]
    lte = client_id_and.filters[1]
    assert gte.operator.value == u">="
    assert gte.field.alias == u"client.id"
    assert gte.value == 5
    assert lte.operator.value == u"<="
    assert lte.field.alias == u"client.id"
    assert lte.value == 12


def test_decode_query_and(api_serializer, project_fields):
    """
    Test with $gte filter & joined field.
    """
    # Order is important to don't break the tests.
    filt = json.loads("""
    {
        "$or": [
            {"client.id": 5},
            {
                "client.id": {
                    "$gte": 12
                }
            }
        ]
    }
    """, object_pairs_hook=OrderedDict)
    root_and = api_serializer.decode_query(
        filt,
        project_fields
    )
    # Because it's a dict into a dict, another level is created.
    # We expect this :
    # And(
    #   Or(
    #       >= 5,
    #       And(<= 12)
    #   )
    # )
    assert len(root_and.filters) > 0 and isinstance(root_and.filters[0], Or)

    client_id_eq = root_and.filters[0].filters[0]
    assert client_id_eq.operator.value == u"="
    assert client_id_eq.field.alias == u"client.id"
    assert client_id_eq.value == 5

    client_id_gte= root_and.filters[0].filters[1].filters[0]
    assert client_id_gte.operator.value == u">="
    assert client_id_gte.field.alias == u"client.id"
    assert client_id_gte.value == 12
