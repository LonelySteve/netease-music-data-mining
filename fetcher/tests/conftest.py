#!/usr/env python3
import pytest

from src.flag import Flag, FlagGroup


@pytest.fixture
def flags():
    original_flags = getattr(
        FlagGroup, f"_{FlagGroup.__name__}__get_this_cls_registered_flags"
    )()

    original_flags_backup = original_flags.copy()

    all_flags = set()

    all_flags.add(Flag("aaa"))
    all_flags.add(Flag("bbb"))
    all_flags.add(Flag("ccc"))

    all_flags.add(Flag("ddd"))
    all_flags.add(Flag("eee", parents="ddd"))
    all_flags.add(Flag("fff", parents="eee"))

    FlagGroup.register(all_flags)

    try:
        yield {flag.name: flag for flag in all_flags}
    finally:
        # 恢复原注册项
        original_flags.clear()
        original_flags.update(original_flags_backup)
