#!/usr/env python3
from contextlib import contextmanager
from functools import partial
from itertools import chain
from threading import RLock
from typing import DefaultDict, FrozenSet, Iterable, List, Optional, Set, Union

from .exceptions import TypeErrorEx
from .util import is_iterable, repr_injector, void


class IncompatibleFlagError(ValueError):
    """不兼容的 Flag 错误"""

    def __init__(self, flag_group: "FlagGroup", flag: "Flag"):
        super().__init__(flag_group, flag)

        self.flag_group = flag_group
        self.flag = flag

    def __str__(self):
        return f"{self.flag} is not compatible with {type(self.flag_group)}"


@repr_injector
class Flag(object):
    __slots__ = ("_parents", "_aliases", "_name")

    def __init__(
        self,
        name: Optional[str] = None,
        parents: Union[str, Iterable[str]] = None,
        aliases: Optional[Union[str, Iterable[str]]] = None,
    ):
        name = name if name else None

        if name is not None:
            name = self.standardized_name(name)

        parents = parents or []
        aliases = aliases or []

        if isinstance(parents, str):
            parents = parents.split("|")
        if isinstance(aliases, str):
            aliases = aliases.split("|")

        parents = frozenset(self.standardized_name(parent) for parent in parents)
        aliases = frozenset(self.standardized_name(alias) for alias in aliases)

        self._name = name
        self._aliases = frozenset(aliases)
        self._parents = frozenset(parents)

    def __str__(self):
        """优先返回主名称，其次返回所有别名"""
        return (
            self.name or f"{', '.join('%r (alias)' % alias for alias in self._aliases)}"
        )

    def __eq__(self, other):
        # 允许与字符串比较，只要字符串指定的名称存在于自己的所有名称里，就返回真
        if isinstance(other, str):
            return other in self.names
        if isinstance(other, Flag):
            return self.names == other.names

        raise TypeErrorEx((Flag, str), other, "other")

    def __hash__(self):
        return hash(self.names) ^ hash(self.parents)

    @staticmethod
    def standardized_name(name: str):
        if not isinstance(name, str):
            raise TypeErrorEx(str, name, name)

        name = name.strip()
        if not name:
            raise ValueError("name can not be empty.")

        if "|" in name:
            raise ValueError(
                f"the name or alias ({name!r}) cannot contain the '|' character."
            )

        return name

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def aliases(self) -> FrozenSet[str]:
        return self._aliases

    @property
    def names(self) -> FrozenSet[str]:
        """
        获取所有可用名称
        --------------
        - 当主名称为 None 时，此属性返回所有别名集合
        - 当主名称不为 None 时，此属性返回主名称 + 所有别名的集合

        :return: frozenset
        """
        if self.name is None:
            return self._aliases

        return frozenset(chain([self.name], self._aliases))

    @property
    def parents(self) -> FrozenSet[str]:
        return self._parents

    def copy(self):
        return Flag(self.name, self.parents, self.aliases)

    def replace(
        self,
        name: Union[str, object, None] = void,
        parents: Union[str, Iterable[str], object, None] = void,
        aliases: Union[str, Iterable[str], object, None] = void,
    ):
        """
        替换指定的值，返回新的标志实例，替换的新值可以为 None
        
        :param name: 指定新的主名称
        :param parents: 指定新的父名称
        :param aliases: 指定别名
        :return: 新的标志实例
        """
        old_values = [self.name, self.parents, self.aliases]
        new_values = [name, parents, aliases]

        return Flag(
            *(new if new != void else old for old, new in zip(old_values, new_values))
        )


class FlagGroupMeta(type):
    def __new__(mcs, name, bases, attrs, **kwargs):
        # 收集本类定义的标志
        cls_new_flags = []
        # 遍历每个属性，对于每一个属性值为 Flag 类型的键值对：
        # - 获取新的 flag 对象（尝试替换旧 flag 对象的 name 属性）
        # - 添加至 new_flags 收集起来
        # - 重设 cls 对应键的值
        for key, value in attrs.copy().items():
            if isinstance(value, Flag):
                new_flag = attrs[key] = value.replace(name=value.name or key)
                cls_new_flags.append(new_flag)

        cls = super().__new__(mcs, name, bases, attrs)
        cls_register_method = getattr(cls, "register")
        # 先注册基类的
        for base in bases[-1::-1]:
            base_flags = getattr(
                base, f"_{cls.__class__.__name__}__get_registered_flags", lambda: None
            )()
            if base_flags:
                cls_register_method(base_flags)
        # 再注册本类的
        cls_register_method(cls_new_flags)

        return cls

    def __setattr__(self, key, value):
        if isinstance(value, Flag):
            raise ValueError(
                "FlagItem type attribute cannot be set to the current class object, "
                "please use inheritance instead."
            )
        super().__setattr__(key, value)

    def __delattr__(self, item):
        if isinstance(getattr(self, item), Flag):
            raise ValueError(
                "FlagItem type attribute cannot be deleted to the current class object"
            )


class FlagGroupCallbackInfo(object):
    __slots__ = ("callback", "is_set", "call_counter")

    def __init__(
        self, callback, is_set: Optional[bool] = None, max_call_count: int = -1
    ):
        if max_call_count < 0:
            max_call_count = -1

        self.callback = callback
        self.is_set = is_set
        self.call_counter = max_call_count


_flags_type = Union[str, Flag, Iterable[Union[str, Flag]]]


@repr_injector
class FlagGroup(metaclass=FlagGroupMeta):
    """
    标志组类
    -----
    - 线程安全
    - 提供一种存储、管理一组标志的机制
    - 提供标志发生变化时回调已注册的可调用对象的能力
    - 继承此类，定义 Flag 类型的类成员以扩充支持的标志
    """

    _lock = RLock()

    def __init__(self, flags: Optional[_flags_type] = None):
        """
        实例化 Flag
        ----------
        - 构造 Flag 实例所用的项必须已经存在于此 Flag 类已注册的项之中，否则会引发 IncompatibleFlagItemError 异常

        >>> FlagGroup.register(Flag(name="233"))
        >>> FlagGroup("233")

        :param flags:
        """
        self._flags = set()
        self._callbacks: DefaultDict[Flag, List[FlagGroupCallbackInfo]] = DefaultDict[
            Flag, List[FlagGroupCallbackInfo]
        ](list)

        if flags:
            self.set(flags)

    def __str__(self):
        return "|".join(f"{flag!s}" for flag in self._flags)

    def __or__(self, other):
        return self.__add__(other)

    def __add__(self, other):
        if not isinstance(other, (Flag, str)):
            raise TypeError(other)

        self.set(other)

        return self

    def __sub__(self, other):
        if not isinstance(other, (Flag, str)):
            raise TypeError(other)

        self.unset(other)

        return self

    def __len__(self):
        return len(self._flags)

    def __iter__(self):
        return iter(self._flags)

    @classmethod
    def _check_flag_conflict(cls, flag: Flag):
        """检查是否有冲突"""
        for name in flag.names:
            for registered_flag in cls.__get_registered_flags():
                if name == registered_flag:
                    raise ValueError(
                        f"the name {name!r} in {flag!r} conflicts with {registered_flag}"
                    )

    @classmethod
    def _check_loop(cls, flag: Flag):
        # 采取 DFS（深度优先算法），由于我们能确保原先的标志组一定不成环，所以只需要确定引入新注册的标志是否会导致成环
        _flag_items_copy = cls.__get_registered_flags().copy()
        _flag_items_copy.add(flag)
        _visit_states = DefaultDict[Flag, bool](default_factory=bool)
        _loop = []
        _collect_loop_nodes = False

        def visit(flag_: Flag):
            nonlocal _collect_loop_nodes

            _visit_states[flag_] = True

            for parent in flag_.parents:
                parent_flag = cls.get_flag(parent)
                # 跳过还未注册的父引用
                if parent_flag not in _flag_items_copy:
                    continue

                if not _visit_states.get(parent_flag, False):
                    visit(parent_flag)
                else:
                    _loop.append(parent_flag)
                    _collect_loop_nodes = True
                    break

            if _loop and flag_ in _loop:
                _collect_loop_nodes = False
            if _collect_loop_nodes:
                _loop.append(flag_)

        visit(flag)

        if _loop:
            raise ValueError(
                f"{flag!r} will cause a circular reference problem ({'<-'.join(_loop)}<-{_loop[0]})"
            )

    @classmethod
    def _check_parent(cls, flags: Iterable[Flag]):
        """检查父名称是否有效，同时也会检查 flags 里每个项的类型是否为 Flag"""
        all_flags = list(chain(flags, cls.__get_registered_flags()))
        # 遍历每一个新 flag，对于每一个新 flag 的每一个父名称，一旦发现有一个不在 all_flags 里，就抛出父名称无效错误
        # 这里不考虑循环引用的问题
        for flag in flags:
            if not isinstance(flag, Flag):
                raise TypeErrorEx(Flag, flag)

            for parent_name in flag.parents:
                if parent_name not in all_flags:
                    raise ValueError(
                        f"{flag!r}'s parent name {parent_name!r} is invalid"
                    )

    @classmethod
    def register(cls, flags: Iterable[Flag]):
        # 检查父名称是否有效
        cls._check_parent(flags)

        for flag in flags:
            if not flag.names:
                raise ValueError(f"{flag!r} must have at least one name or alias")

            _flags = cls.__get_registered_flags()

            # 检查名称和别名是否冲突
            cls._check_flag_conflict(flag)
            # 检查新加入该标志是否会导致环状引用问题
            cls._check_loop(flag)

            _flags.add(flag)
            cls.__set_registered_flags(_flags)

    @contextmanager
    def _work(self):
        backup = self._flags.copy()
        try:
            yield self
        except Exception as e:
            with self._lock:
                self._flags = backup
            raise e

    def copy(self) -> "FlagGroup":
        return FlagGroup(*self)

    def any(self, flags: Iterable[Flag]):
        return bool(next((flag for flag in flags if flag in self), None))

    def all(self, flags: Iterable[Flag]):
        return all((flag in self) for flag in flags)

    @classmethod
    def get_flag(cls, s: str) -> Flag:
        """从字符串形式转换到 FlagItem"""
        if not isinstance(s, str):
            raise TypeErrorEx(str, s, "s")

        s = s.strip()

        result = next(
            (item for item in cls.__get_registered_flags() if s in item.names), None
        )
        if result is None:
            raise ValueError(f"unknown name: {s!r}")

        return result

    def has(self, flag: Union[str, Flag]):
        return flag in self

    def to_flag_objs(self, flags: _flags_type):
        if isinstance(flags, (str, Flag)):
            flags = [flags]
        elif not is_iterable(flags):
            raise TypeErrorEx((str, Flag, Iterable[Union[Flag, str]]), flags, "flags")

        result_set = set()
        # 遍历一遍以收集所有要设置的标志对象
        for flag in flags:
            if isinstance(flag, str):
                for single_flag_name in flag.split("|"):
                    result_set.add(self.get_flag(single_flag_name))
            elif isinstance(flag, Flag):
                result_set.add(flag)
            else:
                raise TypeErrorEx((str, Flag), flag)

        return result_set

    def set(self, flags: _flags_type, set_parent_flag_automatically=True):
        with self._work():  # 支持回滚
            # 遍历设置每一个收集到的标志对象
            for flag in self.to_flag_objs(flags):
                # 检查兼容性
                if flag not in self.__get_registered_flags():
                    raise IncompatibleFlagError(self, flag)
                # 检查父引用，有必要的话，设置父标志到当前标志组
                for parent_name in flag.parents:
                    if (
                        not set_parent_flag_automatically
                        and parent_name not in self._flags
                    ):
                        raise ValueError(
                            f"{flag!r} depends on {parent_name!r}, "
                            f"but it does not exist in the current flag group!"
                        )
                    # 设置父标志
                    self.set(parent_name)
                # 检查冲突
                conflict_group = self._get_conflict_group(self._flags | {flag})
                if conflict_group is not None:
                    raise ValueError(f"{conflict_group!r} cannot coexist.")
                # RLock 获取锁之后加入新标志
                with self._lock:
                    self._flags.add(flag)

    def unset(self, flags: _flags_type):
        with self._work():  # 支持回滚
            # 遍历取消设置每一个收集到的标志对象
            for flag in self.to_flag_objs(flags):
                # 检查要移除的标志是否被依赖
                for f in self._flags - {flag}:
                    if flag in f.parents:
                        # TODO 更好的翻译或者异常
                        raise ValueError(f"{flag!r} is dependent on {f!r}")
                # RLock 获取锁之后移除标志
                with self._lock:
                    if flag in self._flags:  # 允许取消设置一个已经未设置的标志
                        self._flags.remove(flag)

    def emit(self, flag: Union[str, Flag], is_set: bool, *args, **kwargs):
        if is_set:
            self.set(flag)
        else:
            self.unset(flag)

        if isinstance(flag, str):
            flag = self.get_flag(flag)

        callback_infos = self._callbacks[flag]

        for callback_info in callback_infos:
            # 当注册的回调能确定要求是否是 `set` 动作且要求的 `set` 动作与触发的 `set` 动作不一致时放弃调用该回调
            if callback_info.is_set is not None and is_set != callback_info.is_set:
                return

            if callback_info.call_counter < 0:
                callback_info.callback(*args, **kwargs)
            elif callback_info.call_counter > 0:
                try:
                    callback_info.callback(*args, **kwargs)
                finally:
                    callback_info.call_counter -= 1
            else:
                # 计数完成，无需在此调用该注册回调，移除它
                self._callbacks.pop(flag)

    def set_emit(self, item: Union[str, Flag], *args, **kwargs):
        self.emit(item, True, *args, **kwargs)

    def unset_emit(self, item: Union[str, Flag], *args, **kwargs):
        self.emit(item, False, *args, **kwargs)

    def when(
        self,
        item: Union[str, Flag],
        callback=None,
        *,
        is_set: Optional[bool] = None,
        max_call_count=-1,
    ):
        if callback is None:
            return partial(
                self.when, item, is_set=is_set, max_call_count=max_call_count
            )

        if not isinstance(max_call_count, int):
            raise TypeErrorEx(int, max_call_count, "max_call_count")
        if isinstance(item, str):
            item = self.get_flag(item)
        if not isinstance(item, Flag):
            raise TypeErrorEx((str, Flag), item, "item")

        self._callbacks[item].append(
            FlagGroupCallbackInfo(
                callback, is_set=is_set, max_call_count=max_call_count
            )
        )

        return callback

    def on_set(self, item: Union[str, Flag], callback=None):
        return self.when(item, is_set=True, callback=callback)

    def once_set(self, item: Union[str, Flag], callback=None):
        return self.when(item, is_set=True, max_call_count=1, callback=callback)

    def on_unset(self, item: Union[str, Flag], callback=None):
        return self.when(item, is_set=False, callback=callback)

    def once_unset(self, item: Union[str, Flag], callback=None):
        return self.when(item, is_set=False, max_call_count=1, callback=callback)

    def on(self, item: Union[str, Flag], callback=None):
        return self.when(item, callback=callback)

    def once(self, item: Union[str, Flag], callback=None):
        return self.when(item, callback=callback, max_call_count=1)

    def _get_mutex_groups(self):
        return []

    def _get_conflict_group(self, flags: Iterable[Flag]):
        for group in self._get_mutex_groups():
            mutex_group = set(group)
            conflict_group = mutex_group & set(flags)
            if len(conflict_group) > 1:
                return conflict_group

    @classmethod
    def __get_registered_flags(cls) -> Set[Flag]:
        return getattr(cls, "_registered_flags", set())

    @classmethod
    def __set_registered_flags(cls, registered_flags: Set[Flag]):
        setattr(cls, "_registered_flags", registered_flags)


class TaskFlagGroup(FlagGroup):
    pending = Flag(aliases="pend")
    running = Flag(aliases="run")
    stopping = Flag(aliases="stop")
    canceling = Flag(aliases="cancel")
    stopping_with_exception = Flag(parents="stopping", aliases="swe")
    stopping_with_canceled = Flag(parents="stopping", aliases="swc")

    def _get_mutex_groups(self):
        return [
            [self.pending, self.running, self.stopping, self.canceling],
            [self.stopping_with_exception, self.stopping_with_canceled],
        ]
