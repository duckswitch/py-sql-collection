# coding: utf-8
"""
This file contains tests for ApiSerialiser class.
"""

import pytest
from pytest import fixture
from pymongosql.serializer.api_serializer import ApiSerializer
from pymongosql.serializer.api_type import (
    Select,
    Column,
    Table,
    Field
)
from pymongosql.serializer.api_exception import (
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
def dummy_select():
    """
    Initiate object.
    """
    return Select()

@fixture(scope=u"function")
def client_table(api_serializer):
    return Table(
        name=u"client",
        alias=u"client",
        columns=api_serializer.table_columns[u"client"],
        is_root_table=True
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
    filters = api_serializer.decode_query(
        {u"name": u"test"},
        project_fields
    )
    assert filters[0].field.column.name == u"name"
    assert filters[0].operator.value == u"="
    assert filters[0].value == u"test"

def test_decode_query_custom_op(api_serializer, project_fields):
    """
    Test with $gte filter & joined field.
    """
    filters = api_serializer.decode_query(
        {
            u"client.id": {
                u"$gte": 5
            }
        },
        project_fields
    )
    assert filters[0].field.column.name == u"id"
    assert filters[0].field.alias == u"client.id"
    assert filters[0].operator.value == u">="
    assert filters[0].value == 5
