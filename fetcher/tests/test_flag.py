#!/usr/env python3
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.flag import Flag, FlagGroup, IncompatibleFlagError
from tests.utils import override_flag_registered, test_flag_tuple


class TestFlag(object):
    def test_construction(self):
        Flag()
        Flag(name="aaa")
        Flag(name="123")
        Flag(name="bbb", parents="asd|asdas")
        Flag(parents="777", aliases="888")
        Flag(name="cccc", parents="asdasd", aliases="asdas|qweqsad|asda")
        Flag(name="", parents="", aliases="")  # 这是被允许的，等价于填 None

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

    @pytest.mark.parametrize("invalid_value", ("|", "test|", "|test", "for|test"))
    def test_delimiters_check(self, invalid_value):
        with pytest.raises(ValueError):
            Flag(name=invalid_value)

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

    @pytest.mark.parametrize("invalid_value", ("test", Flag("test")))
    def test_incompatible_flag_item_check(self, invalid_value):
        # 字符串如果未找到抛出 ValueError
        # Flag 对象则抛出 IncompatibleFlagError(ValueError)
        if isinstance(invalid_value, str):
            with pytest.raises(ValueError):
                FlagGroup(invalid_value)
        elif isinstance(invalid_value, Flag):
            flag = FlagGroup()
            with pytest.raises(IncompatibleFlagError):
                flag.set(invalid_value)

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
    @pytest.mark.parametrize("flag", test_flag_tuple)
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

    @pytest.mark.parametrize("flag", test_flag_tuple)
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
