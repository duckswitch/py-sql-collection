# coding: utf-8
"""
This module contains various utils function at global usage.
"""


def json_set(item, path, value):
    """
    Set the value corresponding to the path in a dict.
    Arguments:
        item (dict): The object where we want to put a field.
        path (unicode): The path separated with dots to the field.
        value: The value to set on the field.
    Return:
        (dict): The updated object.
    """
    tab = path.split(u".")
    if tab[0] not in item and len(tab) > 1:
        item[tab[0]] = {}
    if len(tab) == 1:
        item[tab[0]] = value
    else:
        item[tab[0]] = json_set(item[tab[0]], u".".join(tab[1:]), value)
    return item


def json_get(item, path, default=None):
    """
    Return the path of the field in a dict.
    Arguments:
        item (dict): The object where we want to put a field.
        path (unicode): The path separated with dots to the field.
        default: default value if path not found.
    Return:
        The value.
    """
    tab = path.split(u".")
    if tab[0] in item:
        if len(tab) > 1:
            return json_get(item[tab[0]], u".".join(tab[1:]), default=default)
        return item[tab[0]]

    return default