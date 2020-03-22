#!/usr/env python3
import re
from collections import defaultdict
from functools import partial
from itertools import count, repeat


def jump_step(start=0, stop=None, base=2, repeat_count=2):
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


def repr_injector(cls=None, include=None, exclude=None, format_dict=None):
    if cls is None:
        return partial(repr_injector, include=include, exclude=exclude, format_dict=format_dict)

    if include is None and exclude is None:
        exclude = [r"^_.*"]
    if include is not None and exclude is not None:
        raise ValueError("Cannot specify both include and exclude.")

    def __repr__(self):
        display_attr_names = None

        if include is not None:
            display_attr_names = [key for key in self.__dict__.keys() if any(re.match(pat, key) for pat in include)]
        elif exclude is not None:
            display_attr_names = [key for key in self.__dict__.keys() if all(not re.match(pat, key) for pat in exclude)]

        attr_display_units = [
            f"{key}={format_dict.get(getattr(self, key), lambda v: '%r' % v)(getattr(self, key))}"
            for key in display_attr_names
        ]

        return f"<{self.__class__.__name__} {' '.join(attr_display_units)}>"

    setattr(cls, "__repr__", __repr__)

    return cls
