#!/usr/env python3
from contextlib import contextmanager
from itertools import chain
from typing import Union

from src.exceptions import TypeErrorEx
from src.flag import Flag, FlagGroup


@contextmanager
def override_flag_registered(*flags: Union[Flag, str]):
    all_flags = []

    for flag in flags:
        if isinstance(flag, Flag):
            all_flags.append(flag)
        elif isinstance(flag, str):
            for flag_ in flag.split("|"):
                all_flags.append(Flag(flag_))
        else:
            raise TypeErrorEx((Flag, str), flag)

    FlagGroup.register(all_flags)

    try:
        yield all_flags
    finally:
        # 强行调用私有方法进行清理
        getattr(FlagGroup, "_FlagGroup__set_registered_flags")(set())


_test_flag_tuple = (
    "test",
    " test ",
    "test1|test2",
    "test1 | test2 | test3",
    Flag("test1"),
    Flag("test2"),
    Flag("test1 "),
    Flag(" test2"),
    Flag(" test "),
)
