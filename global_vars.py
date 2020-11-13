"""
@encoding:utf-8
@author:Tommy
@time:2020/11/12　22:02
@note:
@备注:
"""


def _init():
    global _global_dict
    _global_dict = {}


def set_value(name, value):
    _global_dict[name] = value


def get_value(name, defValue=None):
    try:
        return _global_dict[name]
    except KeyError:
        return defValue
