#!/usr/env python3
from contextlib import contextmanager
from typing import Set, Union

from src.exceptions import TypeErrorEx
from src.flag import Flag, FlagGroup


@contextmanager
def override_flag_registered(*flags: Union[Flag, str], flag_group_cls=FlagGroup):
    original_flags = getattr(
        flag_group_cls, f"_{flag_group_cls.__name__}__get_this_cls_registered_flags"
    )()
    original_flags_backup = original_flags.copy()

    all_flags = set()

    for flag in flags:
        if isinstance(flag, Flag):
            all_flags.add(flag)
        elif isinstance(flag, str):
            all_flags.update(Flag(flag_) for flag_ in flag.split("|"))
        else:
            raise TypeErrorEx((Flag, str), flag)

    flag_group_cls.register(all_flags)

    try:
        yield all_flags
    finally:
        # 恢复原注册项
        original_flags.clear()
        original_flags.update(original_flags_backup)


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
