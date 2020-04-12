#!/usr/env python3
from functools import partial
from itertools import chain
from typing import Tuple, Type

import pytest

from src.event import EventPropagationLevel


class TestEventSystem(object):
    def test_attr_access(self, event_system_case):
        for obj in event_system_case:
            assert obj.aaa.name == "aaa"
            assert obj.bbb.name == "bbb-new-name"

            assert obj.aaa.control_block
            assert obj.bbb.control_block

    @pytest.mark.parametrize("register_type", ("class", "instance"))
    @pytest.mark.parametrize("emit_type", ("class", "instance"))
    @pytest.mark.parametrize("level_on_event", chain([None], EventPropagationLevel))
    @pytest.mark.parametrize(
        "level_on_register_obj", chain([None], EventPropagationLevel)
    )
    def test_event_emit(
        self,
        register_type,
        emit_type,
        capsys,
        event_system_case,
        level_on_event,
        level_on_register_obj,
    ):
        result_count = (
            3 if register_type == "instance" and emit_type == "instance" else 2
        )

        trans = {"class": event_system_case[0], "instance": event_system_case[1]}
        # 类和实例上的事件控制块不是同一对象
        assert id(trans["class"].event_control_block) != id(
            trans["instance"].event_control_block
        )

        register_obj = trans[register_type]
        emit_obj = trans[emit_type]

        # 类上的事件或实例上的事件的事件控制块分别是类上的事件控制块和实例上的事件控制块的引用
        # 故同一类或同一实例上的事件控制块是同一个，对应于类或实例上的事件控制块
        assert (
            register_obj.event_control_block
            is register_obj.aaa.control_block
            is register_obj.bbb.control_block
        )

        register_obj.event_control_block.level = level_on_register_obj
        register_obj.aaa.level = level_on_event

        @register_obj.aaa.on
        def for_test(sender):
            print(f"aaa:{sender}")

        @register_obj.bbb.on
        def for_test(sender):
            print(f"bbb:{sender}")

        emit_obj.aaa.emit(1)
        emit_obj.bbb.emit(2)

        captured = capsys.readouterr()

        get_emit_expect_count = partial(
            self._get_emit_expect_count,
            register_type=register_type,
            emit_type=emit_type,
            register_obj=register_obj,
        )

        assert captured.out.count("aaa:1") == get_emit_expect_count(register_obj.aaa)
        assert captured.out.count("bbb:2") == get_emit_expect_count(register_obj.bbb)

    # 获取 aaa 和 bbb 事件的触发预计数量
    @staticmethod
    def _get_emit_expect_count(event, register_type, emit_type, register_obj):
        if register_type == "instance" and emit_type == "instance":
            if event.level is not None:
                return event.level.value + 1
            if register_obj.event_control_block.level is not None:
                return register_obj.event_control_block.level.value + 1

            return 3
        else:
            if event.level is not None:
                if event.level == EventPropagationLevel.instance:
                    return 0

                return event.level.value
            if register_obj.event_control_block.level is not None:
                if event.level == EventPropagationLevel.instance:
                    return 0

                return register_obj.event_control_block.level.value

            return 2
