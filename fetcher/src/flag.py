#!/usr/env python3
from itertools import chain
from typing import Union, Iterable, Optional

from src.util import repr_injector
from contextlib import contextmanager


@repr_injector
class FlagItem(object):
    __slots__ = ("_flag_cls", "_parents", "aliases", "name")

    def __init__(self, parents: Union[str, Iterable[str]] = None, name: Optional[str] = None,
                 aliases: Optional[Union[str, Iterable[str]]] = None):
        if isinstance(parents, str):
            parents = [parents]
        if isinstance(aliases, str):
            aliases = [aliases]

        parents = set(parents or [])
        aliases = set(aliases or [])

        self.aliases = aliases
        self.name = name

        self._parents = parents

    def __str__(self):
        return self.name or f"{'(alias), '.join('%r' % alias for alias in self.aliases)}"

    @property
    def flag_items(self):
        return (item for key, item in getattr(self, "_flag_cls").__dict__.items() if isinstance(item, FlagItem))

    @property
    def parents(self):
        for parent_name in self._parents:
            for item in self.flag_items:
                if parent_name in item.names:
                    yield item
                    break
            else:
                raise ValueError(f"The parent flag of {self!r} ({parent_name!r}) is invalid.")

        return (item for item in self.flag_items for parent in self._parents if parent in item.names)

    @property
    def names(self):
        if self.name is None:
            return self.aliases
        return chain([self.name], self.aliases)

    def in_parents(self, flag: Union[str, "FlagItem"]):
        if isinstance(flag, str):
            item = next((item for item in self.flag_items if flag in item.names), None)
            if item is None:
                return False
            flag = item

        if not isinstance(flag, FlagItem):
            raise TypeError(flag)

        return flag in self.parents or any(parent.in_parents(flag) for parent in self.parents)


class FlagMeta(type):

    def __init__(cls, name, bases, attrs, **kwargs):
        known_names = set()
        flag_items = []

        for key, value in attrs.items():
            if isinstance(value, FlagItem):
                # 尝试添加默认名称
                value.name = value.name or key
                # 检查名称和别名是否冲突
                conflict_aliases = known_names & set(value.names)
                if conflict_aliases:
                    raise ValueError(f"{value!r} name or alias conflict. ({conflict_aliases!r})")
                # 临时记录到 flag_items 中
                flag_items.append(value)
                # 为 flag item 注入所属的类对象
                setattr(value, "_flag_cls", cls)
                # 将当前 flag item 的名称和别名并入到已知名称中
                list(map(known_names.add, value.names))

        # 检查其父标志引用是否有效
        for item in flag_items:
            for name in item.names:
                if item.in_parents(name):
                    raise ValueError(f"FlagItem ({item!r}) has a circular reference.")

        # 向 Flag 类对象注入其定义的 Flag items，如果其基类也有此对象，则进行 扩展
        for base in bases:
            base_flag_items = getattr(base, "_flag_items", None)
            if base_flag_items:
                flag_items.extend(base_flag_items)
        setattr(cls, "_flag_items", flag_items)

        super().__init__(name, bases, attrs)

    def __setattr__(self, key, value):
        if isinstance(value, FlagItem):
            raise ValueError("FlagItem type attribute cannot be set to the current class object, "
                             "please use inheritance instead.")
        super().__setattr__(key, value)


@repr_injector
class Flag(metaclass=FlagMeta):
    def __init__(self, flags: Optional[Union[Iterable[FlagItem], FlagItem]] = None):
        flags = flags or []
        if isinstance(flags, FlagItem):
            flags = [flags]
        flags = set(flags)

        # 检查类型
        for flag in flags:
            if not isinstance(flag, FlagItem):
                raise TypeError(flag)

        # 检查互斥
        conflict_group = self._get_conflict_group(flags)
        if conflict_group:
            raise ValueError(f"The flags ({conflict_group!r}) cannot coexist")

        self._flags = flags

    def __str__(self):
        return "|".join(f"{flag!s}" for flag in self._flags)

    def __contains__(self, item):
        return item in self._flags

    @contextmanager
    def _work(self):
        backup = self._flags.copy()
        err = None
        try:
            yield self
        except Exception as e:
            err = e
            self._flags = backup
        finally:
            if err:
                raise err

    @property
    def empty(self):
        return len(self._flags) == 0

    def any(self, *flags):
        return any(flag for flag in self._flags if flag in flags)

    def all(self, *flags):
        return all(flag for flag in self._flags if flag in flags)

    def set(self, *flags: Union[str, FlagItem], set_parent_flag_automatically=True):
        # names parents
        # 关键在于处理带 parents 的 flag，这些 flag 需要确定是否自动添加父引用

        with self._work():  # 支持回滚
            for flag in flags:
                if isinstance(flag, str):
                    result = next((item for item in self._flag_items if flag in item.names), None)
                    if result is None:
                        raise ValueError(f"Unknown name: {flag}")
                    flag = result
                if isinstance(flag, FlagItem):
                    # 检查父引用
                    for parent in flag.parents:
                        if not set_parent_flag_automatically and parent not in self._flags:
                            raise ValueError(f"{flag} depends on {parent}, "
                                             f"but it does not exist in the current flag!")
                        # 同样要检查是否存在环状引用
                        if parent.in_parents(flag):
                            raise ValueError(f"({flag!r}) has a circular reference.")
                        self.set(parent)
                    # 检查冲突
                    conflict_group = self._get_conflict_group(self._flags | {flag})
                    if conflict_group is not None:
                        raise ValueError(f"{conflict_group!r} cannot coexist.")
                    self._flags.add(flag)
                else:
                    raise TypeError(flag)

    def unset(self, *flags: Union[str, FlagItem]):
        with self._work():  # 支持回滚
            for flag in flags:
                if isinstance(flag, str):
                    result = next((item for item in self._flag_items if flag in item.names), None)
                    if result is None:
                        raise ValueError(f"Unknown name: {flag}")
                    flag = result
                if isinstance(flag, FlagItem):
                    if flag in self._flags:  # 允许取消设置一个已经未设置的标志
                        self._flags.remove(flag)
                else:
                    raise TypeError(flag)

    def __or__(self, other):
        if not isinstance(other, (FlagItem, str)):
            raise TypeError(other)
        self.set(other)
        return self

    def __add__(self, other):
        return self.__or__(other)

    def __sub__(self, other):
        if not isinstance(other, (FlagItem, str)):
            raise TypeError(other)
        self.unset(other)
        return self

    @property
    def _flag_items(self) -> Iterable[FlagItem]:
        return getattr(self, "_flag_items")

    def _get_mutex_groups(self):
        return []

    def _get_conflict_group(self, flags: Iterable[FlagItem]):
        flags = set(flags)

        for group in self._get_mutex_groups():
            mutex_group = set(group)
            conflict_group = mutex_group & flags
            if len(conflict_group) > 1:
                return conflict_group


class ThreadFlag(Flag):
    pending = FlagItem(aliases="pend")
    running = FlagItem(aliases="run")
    stopping = FlagItem(aliases="stop")
    canceling = FlagItem(aliases="cancel")
    stopping_with_exception = FlagItem(parents="stopping", aliases="swe")
    stopping_with_canceled = FlagItem(parents="stopping", aliases="swc")

    def _get_mutex_groups(self):
        return [[self.pending, self.running, self.stopping, self.canceling],
                [self.stopping_with_exception, self.stopping_with_canceled]]


class JobStepFlag(ThreadFlag):
    stepping = FlagItem(aliases="step")
    leaping = FlagItem(aliases="leap")
    reverse = FlagItem()

    def _get_mutex_groups(self):
        return chain(super()._get_mutex_groups(), [
            [self.stepping, self.leaping]
        ])
