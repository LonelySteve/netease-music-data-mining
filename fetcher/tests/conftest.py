#!/usr/env python3
import pytest
from pyee import BaseEventEmitter, AsyncIOEventEmitter

from src.event import EventSystem, Event
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


@pytest.fixture(params=[None, BaseEventEmitter, AsyncIOEventEmitter])
def event_system_case(request):
    emitter_factory = request.param
    if emitter_factory is not None:
        emitter_factory = lambda: request.param()

    class _ForTestClass(EventSystem, emitter_factory=emitter_factory):
        aaa: Event
        bbb = Event(name="bbb-new-name")

    _for_test_obj = _ForTestClass()

    yield _ForTestClass, _for_test_obj

    del _for_test_obj
    del _ForTestClass

    # 重置 EventSystem 的 cls_emitter
    EventSystem.cls_emitter = getattr(EventSystem, "_emitter_factory")()
