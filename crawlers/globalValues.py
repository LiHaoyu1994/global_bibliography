"""
@encoding:utf-8
@author:Tommy
@time:2020/10/3　22:06
@note:
@备注:
"""


def _init():
    global GLOBAL_DICT
    GLOBAL_DICT = {}


def set_value(name, value):
    GLOBAL_DICT[name] = value


def get_value(name, defValue=None):
    try:
        return GLOBAL_DICT[name]
    except KeyError:
        return defValue
