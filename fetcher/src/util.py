#!/usr/env python3
from collections import defaultdict
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
