#!/usr/env python3
from concurrent.futures import ThreadPoolExecutor
from itertools import chain

import pytest

from src.flag import Flag, FlagGroup, TaskFlagGroup
from src.utils import void
from tests.utils import _test_flag_tuple, override_flag_registered


class TestFlag(object):
    def test_construction(self):
        Flag()
        Flag(name="aaa")
        Flag(name="123")
        Flag(name="bbb", parents="asd|asd")
        Flag(parents="777", aliases="888")
        Flag(name="cccc", parents="asd asd", aliases="asd |qwerty| aaa")
        Flag(name="", parents="", aliases="")  # 这是被允许的，等价于填 None

    @pytest.mark.parametrize("value", ("", " ", "|", " |", "| ", " | ", "   "))
    def test_standardized_name_invalid_case(self, value):
        match_content = "|" if "|" in value else "empty"
        with pytest.raises(ValueError, match=match_content):
            Flag.standardized_name(value)

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
    def test_standardized_name_valid_case(self, value):
        assert Flag.standardized_name(value) == value.strip()

    @pytest.mark.parametrize("name", ("test", " aaa", "bbb ", " ccc ", None))
    @pytest.mark.parametrize(
        "aliases", ("test", " aaa", "bbb ", " ddd ", "aaa|bbb", None)
    )
    def test_names_property(self, name, aliases):
        flag = Flag(name=name, aliases=aliases)

        correct_cases = set()
        if name is not None:
            correct_cases.add(name.strip())
        if aliases is not None:
            correct_cases.update(item.strip() for item in aliases.split("|"))

        assert flag.names == correct_cases

    @pytest.mark.parametrize(
        "invalid_value", (" ", "   ", "|", "asd|", "|test", "|test|")
    )
    def test_empty_name_check(self, invalid_value):
        # 不允许名称中出现 '|'，不允许出现空字符串
        with pytest.raises(ValueError):
            Flag(name=invalid_value)
        with pytest.raises(ValueError):
            Flag(aliases=invalid_value)
        with pytest.raises(ValueError):
            Flag(parents=invalid_value)

    @pytest.mark.parametrize("value", ("|", "test|", "|test", "for|test"))
    def test_delimiters_invalid_case(self, value):
        with pytest.raises(ValueError):
            Flag(name=value)

    @pytest.mark.parametrize("parents", (True, False))
    @pytest.mark.parametrize("aliases", (True, False))
    @pytest.mark.parametrize(
        "value", ("for|test", "aa|bb|cc", ("aa", "bb"), ("aa", "bb", "cc"))
    )
    def test_delimiters_valid_case(self, parents, aliases, value):
        kwargs = {}
        if parents:
            kwargs["parents"] = value
        if aliases:
            kwargs["aliases"] = value

        Flag(**kwargs)

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

    @pytest.mark.parametrize("attr", ("parents", "aliases"))
    @pytest.mark.parametrize(
        "value",
        [
            "test",
            " test ",
            "test1|test2|test3" " test1 |test2 | test3",
            ("test1", "test2", "test3"),
            (" test1", "test2 ", " test3 "),
        ],
    )
    def test_multi_father(self, attr, value):
        item = Flag(**{attr: value})
        if isinstance(value, str):
            value = value.split("|")

        value = [p.strip() for p in value]

        assert set(getattr(item, attr)) == set(value)

    @pytest.mark.parametrize("name", ("test", "test ", " test"))
    @pytest.mark.parametrize(
        "aliases",
        ("test1", "test2 ", (" test1", "test2 "), "test1|test2", "test1 | test2"),
    )
    def test_equals(self, name, aliases):
        item = Flag(name, aliases=aliases)
        if isinstance(aliases, str):
            aliases = aliases.split("|")
        names = [name.strip()] + [p.strip() for p in aliases]
        for name in names:
            assert name == item
        assert set(names) == set(item.names)

    def test_not_equals(self):
        item = Flag("test", aliases="test1|test2")
        assert item != "test~"
        assert item != Flag("test~")


class TestFlagGroup(object):
    def test_construction_type_check(self):
        FlagGroup()
        with pytest.raises(TypeError):
            FlagGroup(eval("123"))

    @pytest.mark.parametrize("value", ("test", "test|foo|bar"))
    def test_len_magic_method(self, value):
        with override_flag_registered(value) as all_flags:
            flag_group = FlagGroup(all_flags)
            assert len(flag_group) == len(all_flags)

    @pytest.mark.parametrize("value", ("test", "test|foo|bar"))
    def test_iter_magic_method(self, value):
        with override_flag_registered(value) as all_flags:
            flag_group = FlagGroup(all_flags)
            assert set(flag_group) == set(all_flags)

    def test_loop_check(self):
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

    @pytest.mark.parametrize("invalid_value", ("test", Flag("test")))
    def test_incompatible_flag_item_check(self, invalid_value):
        # 如果未找到抛出 ValueError
        if isinstance(invalid_value, str):
            with pytest.raises(ValueError):
                FlagGroup(invalid_value)
        elif isinstance(invalid_value, Flag):
            with pytest.raises(ValueError):
                FlagGroup().set(invalid_value)

    @pytest.mark.parametrize(
        "set_style",
        (
            lambda flag_group, values: flag_group.set(values),
            lambda flag_group, values: flag_group + values,
            lambda flag_group, values: flag_group | values,
        ),
    )
    @pytest.mark.parametrize(
        "unset_style",
        (lambda flag, values: flag.unset(values), lambda flag, values: flag - values),
    )
    @pytest.mark.parametrize("flag", _test_flag_tuple)
    def test_set_and_unset(self, set_style, unset_style, flag):
        with override_flag_registered(flag) as all_flags:
            # 构造时指定
            flag_group = FlagGroup(flag)

            for item in all_flags:
                assert item in flag_group

            # 其他方法
            flag_group = FlagGroup()
            set_style(flag_group, flag)

            for item in all_flags:
                assert item in flag_group

            unset_style(flag_group, flag)

            for item in all_flags:
                assert item not in flag_group

    @pytest.mark.parametrize("flag", _test_flag_tuple)
    def test_any_and_all(self, flag):
        with override_flag_registered(flag) as all_flags:
            flag = FlagGroup(flag)
            assert flag.all(all_flags)
            for item in flag:
                flag = FlagGroup(item)
                assert flag.any(all_flags)

    def test_conflict_items(self):
        with pytest.raises(ValueError, match="conflict"):
            with override_flag_registered("test|test"):
                ...

        with override_flag_registered("aaa"):
            with pytest.raises(ValueError, match="conflict"):

                class _TestFlagGroup(FlagGroup):
                    aaa = Flag()
                    bbb = Flag()

            with pytest.raises(ValueError, match="conflict"):

                class _TestFlagGroupAlias(FlagGroup):
                    bbb = Flag(aliases="aaa")

    @pytest.mark.parametrize("is_set", (None, True, False))
    def test_emitter_set_or_not_set(self, is_set):
        with override_flag_registered("test"):
            flag = FlagGroup()

            triggered = False

            @flag.when("test", is_set=is_set, max_call_count=-1)
            def for_test_closure():
                nonlocal triggered
                triggered = True

            if is_set is None:
                assert not triggered
                flag.set_emit("test")
                assert triggered
                triggered = False
                flag.unset_emit("test")
                assert triggered
                return

            if is_set:
                assert not triggered
                flag.set_emit("test")
                assert triggered
                triggered = False
                flag.unset_emit("test")
                assert not triggered
            else:
                flag.set_emit("test")
                assert not triggered
                flag.unset_emit("test")
                assert triggered

    @pytest.mark.parametrize("max_call_count", (-1, 0, 1, 5))
    def test_emitter_call_constraint(self, max_call_count):
        with override_flag_registered("test"):
            flag = FlagGroup()

            trigger_counter = 0

            @flag.when("test", max_call_count=max_call_count)
            def for_test_closure():
                nonlocal trigger_counter
                trigger_counter += 1

            if max_call_count == -1:
                flag.set_emit("test")
                assert trigger_counter == 1
            else:
                for _ in range(max_call_count):
                    flag.set_emit("test")
                assert trigger_counter == max_call_count

    def test_emitter_call_with_arguments(self):
        with override_flag_registered("test"):
            flag = FlagGroup()

            args = None
            kwargs = None

            @flag.when("test", is_set=None, max_call_count=-1)
            def for_test_closure(*args_, **kwargs_):
                nonlocal args
                nonlocal kwargs
                args = args_
                kwargs = kwargs_

            assert args is None and kwargs is None

            flag.set_emit("test", 1, 2, 3, test="for_test")

            assert args == (1, 2, 3)
            assert kwargs == {"test": "for_test"}

    def test_set_and_unset_under_multithreading(self):
        with override_flag_registered("test"):
            flag_group = FlagGroup()

            def job(*args, **kwargs):
                # 重复设置标志，重复取消标志都没问题，标志组里的每个标志都是唯一的，不会重复添加，也不会重复取消
                for _ in range(3):
                    flag_group.set("test")
                for _ in range(2):
                    flag_group.unset("test")

            with ThreadPoolExecutor() as executor:
                executor.map(job, range(5))

            assert "test" not in flag_group

    def test_set_and_unset_conflict(self):
        flag_group = TaskFlagGroup(TaskFlagGroup.pending)

        assert flag_group.has(TaskFlagGroup.pending)

        with pytest.raises(ValueError, match="stopping"):
            flag_group.set(TaskFlagGroup.stopping)

    def test_parents_invalid_case(self):
        with pytest.raises(ValueError, match="invalid"):

            class _TestFlagGroup(FlagGroup):
                aaa = Flag(parents="bbb")
