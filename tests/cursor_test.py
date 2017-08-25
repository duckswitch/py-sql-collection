# coding: utf-8
"""
This file contains tests for Cursor class.
"""

import json
from pytest import fixture
from mock import Mock
from pysqlcollection.cursor import Cursor


@fixture(scope=u"function")
def cursor():
    return Cursor(
        Mock(), Mock(), Mock(), Mock()
    )


def test_deduplicate_lookup(cursor):
    items = [
        {
            u"id": 2.0,
            u"categories":
                {
                    u"door_hotspot_category": 2.0,
                    u"door": 2.0,
                    u"id": 2.0
                }
        }, {
            u"id": 2.0,
            u"categories":
                {
                    u"door_hotspot_category": 3.0,
                    u"door": 2.0,
                    u"id": 3.0
                }
        }, {
            u"id": 2.0,
            u"categories":
                {
                    u"door_hotspot_category": 1.0,
                    u"door": 2.0,
                    u"id": 4.0
                }
        }, {
            u"id": 3.0,
            u"categories":
                {
                    u"door_hotspot_category": 2.0,
                    u"door": 3.0,
                    u"id": 2.0
                }
        }
    ]

    result = cursor._deduplicate(items, prim_key=u"id", duplicated=u"categories", foreign_primary_key=u"id")

    assert result == [
        [
            {
                u"door_hotspot_category": 2.0,
                u"door": 2.0,
                u"id": 2.0
            }, {
                u"door_hotspot_category": 3.0,
                u"door": 2.0,
                u"id": 3.0
            }, {
                u"door_hotspot_category": 1.0,
                u"door": 2.0,
                u"id": 4.0
            }
        ], [
            {
                u"door_hotspot_category": 2.0,
                u"door": 3.0,
                u"id": 2.0
            }
        ]
    ]