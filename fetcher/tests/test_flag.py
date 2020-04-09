#!/usr/env python3
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from itertools import combinations
from operator import and_, or_, xor
from threading import Thread

import pytest
from pyee import BaseEventEmitter

from src.flag import Flag, FlagGroup, TaskFlagGroup
from src.utils import void

from .utils import assert_time_cost


class TestFlag(object):
    _test_init_common = (
        "",
        None,
        "aaa",
        " aaa ",
        "aaa|aaa",
        " aaa | aaa ",
        "aaa|bbb",
        " aaa | bbb ",
        ("aaa",),
        ("aaa", "aaa"),
        (" aaa ",),
        (" aaa ", " aaa "),
        ("aaa", "bbb"),
        (" aaa ", " bbb "),
    )
    _test_init_raise_value_error = (None, "aaa|", "|bbb")

    @pytest.mark.parametrize("name", ("aaa", None, "", " aaa", "aaa ", " aaa "))
    @pytest.mark.parametrize("parents", _test_init_common)
    @pytest.mark.parametrize("aliases", _test_init_common)
    def test_init(self, name, parents, aliases):
        flag = Flag(name=name, parents=parents, aliases=aliases)

        if not name:
            assert flag.name is None
        if not parents:
            assert flag.parents == frozenset()
        if not aliases:
            assert flag.aliases == frozenset()

        if name:
            assert flag.name == name.strip()
        if parents:
            if isinstance(parents, str):
                assert flag.parents == frozenset(
                    item.strip() for item in parents.split("|")
                )
            else:
                assert flag.parents == frozenset(item.strip() for item in parents)
        if aliases:
            if isinstance(aliases, str):
                assert flag.aliases == frozenset(
                    item.strip() for item in aliases.split("|")
                )
            else:
                assert flag.aliases == frozenset(item.strip() for item in aliases)

    @pytest.mark.parametrize(
        "name", (None, " ", "  ", "|", " |", "| ", "aaa|", "aaa|bbb")
    )
    @pytest.mark.parametrize("parents", _test_init_raise_value_error)
    @pytest.mark.parametrize("aliases", _test_init_raise_value_error)
    def test_init_raise_value_error(self, name, parents, aliases):

        if all(arg is None for arg in [name, parents, aliases]):
            pytest.skip("所有参数均为 None，为正常情况，不会抛出异常")

        with pytest.raises(ValueError):
            Flag(name=name, parents=parents, aliases=aliases)

    @pytest.mark.parametrize(
        "value",
        (
                "test",
                " test",
                "test ",
                " test ",
                "for test",
                " for test",
                "for test ",
                " for test ",
        ),
    )
    def test_standardized_name(self, value):
        assert Flag.standardized_name(value) == value.strip()

    @pytest.mark.parametrize("value", ("", " ", "|", " |", "| ", " | ", "   "))
    def test_standardized_name_raise_value_error(self, value):
        match_content = "|" if "|" in value else "empty"
        with pytest.raises(ValueError, match=match_content):
            Flag.standardized_name(value)

    @pytest.mark.parametrize("name", ("aaa", None, ""))
    @pytest.mark.parametrize("aliases", ("aaa", "aaa|bbb", ("aaa", "bbb"), None, ""))
    def test_names_property(self, name, aliases):
        flag = Flag(name=name, aliases=aliases)

        correct_cases = set()
        if name:
            correct_cases.add(name)
        if aliases:
            if isinstance(aliases, str):
                correct_cases.update(alias.strip() for alias in aliases.split("|"))
            else:
                correct_cases.update(alias.strip() for alias in aliases)

        assert flag.names == correct_cases

    @pytest.mark.parametrize("name", (void, "test"))
    @pytest.mark.parametrize("parents", (void, "test", "aaa|bbb", ("aaa", "bbb")))
    @pytest.mark.parametrize("aliases", (void, "test", "aaa|bbb", ("aaa", "bbb")))
    def test_replace(self, name, parents, aliases):
        flag = Flag(name="name", parents="parents", aliases="aliases")
        new_flag = flag.replace(name=name, parents=parents, aliases=aliases)

        arg_values = [name, parents, aliases]
        old_values = [flag.name, flag.parents, flag.aliases]
        new_values = [new_flag.name, new_flag.parents, new_flag.aliases]

        for i, arg in enumerate(arg_values):
            if i == 0 or arg == void:
                continue
            if isinstance(arg, str):
                arg = arg.split("|")

            arg_values[i] = set(arg)

        for arg, old, new in zip(arg_values, old_values, new_values):
            if arg == void:
                assert old == new
            else:
                assert arg == new

    @pytest.mark.parametrize("attr", ("name", "aliases", "parents"))
    def test_readonly_property(self, attr):
        with pytest.raises(AttributeError):
            setattr(Flag(), attr, "233")

    @pytest.mark.parametrize("strict", (True, False))
    def test_equals(self, strict):
        flag = Flag(name="aaa", aliases="bbb|ccc", parents="ddd")

        equals = partial(flag.equals, strict=strict)

        assert equals(Flag(name="aaa", aliases="bbb|ccc", parents="ddd"))  # 与 strict 无关
        # 使用了异或处理，当 strict 为 True，式子右边的值都应为 False，反之，右值都应为 True
        # 也就是说**两边一定不相等**
        assert strict != equals(Flag(name="aaa", aliases="bbb|ccc"))
        assert strict != equals("aaa")
        assert strict != equals("bbb")
        assert strict != equals("ccc")


class TestFlagGroup(object):
    def test_init(self, flags):
        flag_group = FlagGroup()

        assert len(flag_group.flags) == 0

        flag_group = FlagGroup(flags)

        assert flag_group.flags == set(flags.values())

    def test_init_raise_type_error(self):
        with pytest.raises(TypeError):
            FlagGroup(eval("123"))

    def test_init_raise_value_error(self):
        with pytest.raises(ValueError, match="unknown"):
            FlagGroup("aaa")
        with pytest.raises(ValueError, match="compatible"):
            FlagGroup(Flag("aaa"))

    def test_check_loop(self):
        with pytest.raises(ValueError, match="aaa<-bbb<-aaa"):
            class _TestFlagGroup_0(FlagGroup):
                aaa = Flag(parents="bbb")
                bbb = Flag(parents="aaa")

        with pytest.raises(ValueError, match="aaa<-ccc<-bbb<-aaa"):
            class _TestFlagGroup_1(FlagGroup):
                aaa = Flag(parents="bbb")
                bbb = Flag(parents="ccc")
                ccc = Flag(parents="aaa")

        # 这种情况下的会有两种成环结果：
        # - bbb<-ccc<-bbb
        # - aaa<-ccc<-bbb<-aaa
        # 两种结果均是正确的，由于标志组采用集合进行内部实现，所以具体会抛出哪种成环结果并不确定，两种均有可能
        with pytest.raises(
                ValueError, match=r"(?:bbb<-ccc<-bbb)|(?:aaa<-ccc<-bbb<-aaa)"
        ):
            class _TestFlagGroup_2(FlagGroup):
                aaa = Flag(parents="bbb")
                bbb = Flag(parents="ccc")
                ccc = Flag(parents="aaa|bbb")

    def test_flags_property_readonly(self, flags):
        flag_group = FlagGroup(flags)
        backup = flag_group.flags.copy()
        flag_group.flags.clear()

        assert flag_group.flags == backup

    @pytest.mark.parametrize("op", (and_, or_, xor))
    def test_op(self, op, flags):
        flag_group_0 = FlagGroup("aaa")
        flag_group_1 = FlagGroup("aaa|bbb|ccc")

        new_flag_group = flag_group_0.__op__(flag_group_1, op)

        assert flag_group_0 is not new_flag_group and flag_group_1 is not new_flag_group

        assert new_flag_group.flags == op(
            FlagGroup.to_flag_objs(flag_group_0.flags),
            FlagGroup.to_flag_objs(flag_group_1.flags),
        )

    def test_set(self, flags):
        flag_group = FlagGroup()

        assert len(flag_group.flags) == 0

        flag_group.set("aaa")

        assert flag_group.flags == {flags["aaa"]}

        flag_group.set("eee")

        assert flag_group.flags == {flags[name] for name in ["aaa", "ddd", "eee"]}

    def test_set_raise_value_error(self, flags):
        flag_group = FlagGroup()

        with pytest.raises(ValueError, match="unknown"):
            flag_group.set("ggg")

        with pytest.raises(ValueError, match="compatible"):
            flag_group.set(Flag("ggg"))

        with pytest.raises(ValueError, match="depends on"):
            flag_group.set("eee", set_parent_flag_automatically=False)

    def test_unset(self, flags):
        flag_group = FlagGroup(flags)

        assert flag_group.flags == set(flags.values())

        flag_group.unset("aaa")

        assert flags["aaa"] not in flag_group.flags

        flag_group.unset("ddd", remove_all_children=True)

        assert flag_group.flags == {flags["bbb"], flags["ccc"]}

    def test_unset_raise_value_error(self, flags):
        flag_group = FlagGroup(flags)

        assert flag_group.flags == set(flags.values())

        with pytest.raises(ValueError, match="depends on"):
            flag_group.unset("ddd", remove_all_children=False)

    def test_len_magic_method(self, flags):
        flag_group = FlagGroup(flags)
        assert len(flag_group) == len(flags)

    def test_iter_magic_method(self, flags):
        flag_group = FlagGroup(flags)

        assert set(flag_group) == set(flags.values())

    def test_contains_magic_method(self, flags):
        flag_group = FlagGroup(flags)
        for flag in flags.values():
            assert flag in flag_group

    def test_all(self, flags):
        flag_group = FlagGroup("aaa|bbb|ccc")

        assert flag_group.all("aaa")
        assert not flag_group.all("aaa", strict=True)
        assert not flag_group.all("aaa|eee")

        flag_group = FlagGroup(flags)

        for i in range(0, len(flags) + 1):
            for fs in combinations(flags, i):
                assert flag_group.all(fs)

        assert flag_group.all(flags, strict=True)

        for i in range(0, len(flags)):
            for fs in combinations(flags, i):
                assert not flag_group.all(fs, strict=True)

    def test_all_raise_value_error(self):
        flag_group = FlagGroup()

        with pytest.raises(ValueError, match="compatible"):
            flag_group.all([Flag("aaa")])

        with pytest.raises(ValueError, match="unknown"):
            flag_group.all("aaa")

    def test_any(self, flags):
        flag_group = FlagGroup("aaa|bbb|ccc")

        assert flag_group.any("aaa")
        assert flag_group.any("aaa|eee|fff")
        for i in range(1, 3 + 1):
            for group in combinations(["ddd", "eee", "fff"], i):
                assert not flag_group.any(group)

        flag_group = FlagGroup(flags)

        for i in range(0, len(flags) + 1):
            for fs in combinations(flags, i):
                if not fs:
                    assert not flag_group.any(fs)
                else:
                    assert flag_group.any(fs)

    def test_any_raise_value_error(self):
        flag_group = FlagGroup()

        with pytest.raises(ValueError, match="compatible"):
            flag_group.any([Flag("aaa")])

        with pytest.raises(ValueError, match="unknown"):
            flag_group.any("aaa")

    def test_register(self):
        FlagGroup.register([Flag("ggg")])
        FlagGroup("ggg")

    def test_register_raise_value_error(self, flags):
        with pytest.raises(ValueError, match="conflict"):
            FlagGroup.register([Flag("aaa")])

        with pytest.raises(ValueError, match="conflict"):
            class _TestFlagGroup(FlagGroup):
                aaa = Flag()
                ggg = Flag()

        with pytest.raises(ValueError, match="conflict"):
            class _TestFlagGroupAlias(FlagGroup):
                ggg = Flag(aliases="aaa")

        with pytest.raises(ValueError, match="invalid"):
            class _TestFlagGroupParents(FlagGroup):
                ggg = Flag(parents="hhh")

    def test_set_and_unset_under_multithreading(self, flags):
        flag_group = FlagGroup()

        def job(*args, **kwargs):
            # 重复设置标志，重复取消标志都没问题，标志组里的每个标志都是唯一的，不会重复添加，也不会重复取消
            for _ in range(3):
                flag_group.set("aaa")
            for _ in range(2):
                flag_group.unset("aaa")

        with ThreadPoolExecutor() as executor:
            executor.map(job, range(5))

        assert "aaa" not in flag_group

    def test_inherit_flag_group(self):
        class _TestFlagGroup(FlagGroup):
            aaa = Flag()
            bbb = Flag()

        flag_group = _TestFlagGroup(("aaa", "bbb"))

        assert flag_group.flags == {_TestFlagGroup.aaa, _TestFlagGroup.bbb}

        class _TestFlagGroupEx(_TestFlagGroup):
            ccc = Flag()

        flag_group_ex = _TestFlagGroupEx(("aaa", "bbb", "ccc"))

        assert flag_group_ex.flags == {
            _TestFlagGroupEx.aaa,
            _TestFlagGroupEx.bbb,
            _TestFlagGroupEx.ccc,
        }

        class _TestFlagGroupEx2(_TestFlagGroup):
            ddd = Flag()

        flag_group_ex2 = _TestFlagGroupEx2(("aaa", "bbb", "ddd"))

        assert flag_group_ex2.flags == {
            _TestFlagGroupEx2.aaa,
            _TestFlagGroupEx2.bbb,
            _TestFlagGroupEx2.ddd,
        }

        flag_group.set(_TestFlagGroupEx.aaa)
        flag_group_ex.set(_TestFlagGroup.aaa)
        flag_group_ex2.set(_TestFlagGroupEx2.aaa)

        with pytest.raises(ValueError, match="compatible"):
            flag_group.set(_TestFlagGroupEx.ccc)

        with pytest.raises(ValueError, match="compatible"):
            flag_group_ex2.set(_TestFlagGroupEx.ccc)

    def test_copy(self, flags):
        flag_group_0 = FlagGroup("aaa")
        flag_group_1 = flag_group_0.copy()
        assert flag_group_0 is not flag_group_1
        assert flag_group_0.flags == flag_group_1.flags

    def test_equals(self, flags):
        flag_group_0 = FlagGroup("aaa", emitter=BaseEventEmitter())
        flag_group_1 = FlagGroup("aaa", emitter=BaseEventEmitter())

        assert flag_group_0.equals(flag_group_1, strict=False)
        assert not flag_group_0.equals(flag_group_1, strict=True)

    def test_no_wait(self, flags):
        flag_group = FlagGroup()

        def foo():
            time.sleep(0.1)
            flag_group.set("aaa")
            assert flag_group.flags == {flags["aaa"]}
            flag_group.no_wait("bbb")
            assert flag_group.flags == {flags["aaa"], flags["bbb"]}

        def bar():
            assert flag_group.flags == set()
            flag_group.no_wait("aaa")
            assert flag_group.flags == {flags["aaa"]}
            time.sleep(0.1)
            flag_group.set("bbb")
            assert flag_group.flags == {flags["aaa"], flags["bbb"]}

        t1 = Thread(target=foo)
        t2 = Thread(target=bar)

        with assert_time_cost(lambda t: t >= 0.2):
            t1.start()
            t2.start()
            t1.join()
            t2.join()

    def test_wait(self, flags):
        flag_group = FlagGroup("aaa|bbb")

        def foo():
            time.sleep(0.1)
            flag_group.unset("aaa")
            assert flag_group.flags == {flags["bbb"]}
            flag_group.wait("bbb")
            assert flag_group.flags == set()

        def bar():
            assert flag_group.flags == {flags["aaa"], flags["bbb"]}
            flag_group.wait("aaa")
            assert flag_group.flags == {flags["bbb"]}
            time.sleep(0.1)
            flag_group.unset("bbb")
            assert flag_group.flags == set()

        t1 = Thread(target=foo)
        t2 = Thread(target=bar)

        with assert_time_cost(lambda t: t >= 0.2):
            t1.start()
            t2.start()
            t1.join()
            t2.join()


class TestTaskFlagGroup(object):
    def test_set_and_unset_conflict(self):
        flag_group = TaskFlagGroup(TaskFlagGroup.pending)

        assert flag_group.has(TaskFlagGroup.pending)

        with pytest.raises(ValueError, match="stopping"):
            flag_group.set(TaskFlagGroup.stopping)
