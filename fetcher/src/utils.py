#!/usr/env python3
import math
import traceback
from functools import partial
from inspect import isfunction, isgenerator, ismethod
from itertools import count, repeat
from typing import Any, Callable, Generic, Optional, TypeVar

T = TypeVar("T")

void = object()


def jump_step(*, start=0, stop=None, base=2, repeat_count=2):
    """
    跳跃步进迭代器

    :param start: 开始指数值
    :param stop: 结束指数值
    :param base: 底数
    :param repeat_count: 幂运算结果重复次数，默认为 2
    :return:
    """
    for exp in count(start):
        if stop and exp == stop:
            break
        yield from repeat(base ** exp, repeat_count)


def repr_injector(
    cls: Optional[T] = None,
    filter_: Callable[[str, Any], bool] = None,
    format_dict=None,
) -> T:
    if cls is None:
        return partial(repr_injector, filter_=filter_, format_dict=format_dict)

    format_dict = {}
    filter_ = filter_ or (
        # 筛选不以 '_' 开头的非函数、方法、生成器成员
        lambda k, v: not k.startswith("_")
        and not any(is_(v) for is_ in [isgenerator, isfunction, ismethod])
    )

    def safe_get_value(obj, key):
        try:
            result = getattr(obj, key)
        except Exception as e:
            return e
        return result

    def __repr__(self):

        display_attr_names = [
            key for key in dir(self) if filter_(key, safe_get_value(self, key))
        ]

        attr_display_units = [
            f"{key}={format_dict.get(key, lambda v: '%r' % v)(safe_get_value(self, key))}"
            for key in display_attr_names
        ]

        return f"<{self.__class__.__name__} {' '.join(attr_display_units)}>"

    setattr(cls, "__repr__", __repr__)

    return cls


def get_traceback_text(ex_type, ex_obj, tb, *, limit=None):
    if ex_type is None or ex_obj is None or tb is None:
        return None

    return "".join(traceback.format_exception(ex_type, ex_obj, tb, limit=limit))


def is_iterable(obj):
    try:
        iter(obj)
        return True
    except TypeError:
        return False


sign = partial(math.copysign, 1)
