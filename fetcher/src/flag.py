#!/usr/env python3
from contextlib import contextmanager
from itertools import chain
from operator import and_, or_, xor
from threading import Condition
from typing import (
    Callable,
    DefaultDict,
    FrozenSet,
    Generator,
    Iterable,
    Optional,
    Set,
    Union,
)

from pyee import BaseEventEmitter

from .event import EmitterMixin, EventFactory
from .exceptions import TypeErrorEx
from .utils import is_iterable, void


class Flag(object):
    """
    标志类
    -----
    标志主要提供一组信息，用来描述自己的名称（或别名），或提供自己依赖的父标志名称（或别名）。

    这是一个会被冻结的类，一旦构造出 Flag 对象，它的属性便无法被赋值（只读），
    可以通过 ``replace`` 方法替换对象的相应的值并生成一个新的 Flag 对象绕过这个限制。

    关于名称（name）和别名（aliases）：

    - 两者差别不大，传入的参数（或经过分割）都会被去掉前后空白
    - name 之中不能包含 '|' 字符，而 aliases 可以，并且 aliases 包含 '|' 字符时，将自动分割以提取多个别名
    - 当对 Flag 的实例使用 ``str()`` 内建方法时，如果名称不为空则优先返回名称，否则返回所有别名
    - 无论如何，都可以从 ``names`` 属性中获取标志可用的名称或别名

    父名称（parents）类似别名（aliases），在构造时同样支持使用 '|' 进行分割的写法，也可以传入可迭代出字符串的对象以指定

    注意：Flag 类不会在构造时检查传入的父名称的有效性（仅做基本检查，如是否为空），这个过程交给调用方自行处理。

    对于参数完全一致的新 Flag 实例是相等的：

    >>> flag = Flag(name="aaa", aliases="bbb|ccc", parents="ddd")
    >>> assert flag == Flag(name="aaa", aliases=("bbb", "ccc"))

    支持使用字符串与 Flag 实例进行比较：

    >>> assert flag == "aaa"
    >>> assert flag == "bbb" and flag == "ccc"

    值得注意的是，由于 Python 没有 __req__，所以我无法控制字符串在等号左边的情形，这个时候会调用 str 的 __eq__ 方法，内部的实现肯定会返回 False

    >>> "aaa" == flag
    False

    因此，``in`` 操作符正确使用的姿势是：

    >>> flag in ["aaa", "bbb", "ccc"]
    True

    而不是：

    >>> "aaa" in [flag, flag, flag]
    False

    为了避免出现错误的结果，请尽可能使用下面的方案代替上面错误的比较

    >>> flag.equals("aaa")
    True
    >>> bool(next((f.equals("aaa") for f in [flag, flag, flag]), None))
    True

    默认的比较是不会比较父标志是否相同的，如果想要比较父标志，可以将 equals 方法的 strict 参数设置为 True

    >>> flag.equals(Flag(name="aaa", aliases=("bbb", "ccc")), strict=True)
    False

    *注意：为了方便与字符串进行比较，== 重载内部实现用的 equals 方法的 strict 参数为 False

    """

    __slots__ = ("_parents", "_aliases", "_name")

    def __init__(
        self,
        name: Optional[str] = None,
        parents: Optional[Union[str, Iterable[str]]] = None,
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
        """优先返回主名称，其次返回第一个别名"""
        return self.name or next(iter(self._aliases))

    def __repr__(self):
        return f"<{self.__class__.__name__} {str(self)}>"

    def __eq__(self, other):
        # 为了方便与字符串进行比较，使用非严格模式
        return self.equals(other, strict=False)

    def __hash__(self):
        return hash(self.names) ^ hash(self.parents)

    @staticmethod
    def standardized_name(name: str):
        """
        标准化名称，去除名称前后的空白，并验证有效性
        
        :param name: 要标准化的名称
        :type name: str
        :raises TypeErrorEx: 当传入的名称不为 str 类型时抛出
        :raises ValueError: 当名称为空或含有'|'字符时抛出
        :return: 被标准化后的名称
        :rtype: str
        """
        if not isinstance(name, str):
            raise TypeErrorEx(str, name, name)

        name = name.strip()
        if not name:
            raise ValueError("the name or alias can not be empty.")

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

    def equals(self, other, *, strict=True):
        """
        判断此标志与指定对象是否相等

        - strict 为真时: other 参数仅针对 Flag 类型，除了比较两者 names 属性是否相等之外，
          还会比较两者的 parents 是否相等，其他类型永远返回 False
        - strict 为假时: other 参数除了接受 Flag 类型外，也可以指定字符串。
          当 other 参数为字符串类型时，只要保证它在此标志的 names 属性中即返回 True；
          当 other 参数为 Flag 类型时，只要保证两者的 names 属性相等即返回 True

        :param other:
        :param strict:
        :return:
        """
        if not strict and isinstance(other, str):
            # 非严格模式下允许与字符串比较，只要字符串指定的名称存在于自己的所有名称里，就返回真
            return other in self.names
        if isinstance(other, Flag):
            if strict:
                return self.names == other.names and self.parents == other.parents
            else:
                return self.names == other.names

        return False


class FlagGroupMeta(type):
    def __new__(mcs, name, bases, attrs, **kwargs):
        # 收集当前构造的类对象定义的标志
        cls_new_flags = []
        # 遍历每个属性，对于每一个属性值为 Flag 类型的键值对：
        # - 获取新的 flag 对象（尝试替换旧 flag 对象的 name 属性）
        # - 重设 cls 对应键的值
        # - 添加至 new_flags 收集起来
        for key, value in attrs.copy().items():
            if isinstance(value, Flag):
                new_flag = attrs[key] = value.replace(name=value.name or key)
                cls_new_flags.append(new_flag)

        cls = super().__new__(mcs, name, bases, attrs)
        # 获取当前构造的类的 register 方法，并注册
        cls_register_method = getattr(cls, "register")
        cls_register_method(cls_new_flags)

        return cls

    def __setattr__(self, key, value):
        if isinstance(value, Flag):
            raise ValueError(
                "`Flag` type attribute cannot be set to the current class object, "
                "please use inheritance instead"
            )
        super().__setattr__(key, value)

    def __delattr__(self, item):
        if isinstance(getattr(self, item), Flag):
            raise ValueError("`Flag` type attribute cannot be deleted")
        super().__delattr__(item)


_flags_type = Union[str, Flag, Iterable[Union[str, Flag]]]


class FlagGroup(EmitterMixin, metaclass=FlagGroupMeta):
    """
    标志组类
    -------
    标志组主要是一种存储、管理一组标志（Flag 类的实例）的容器，并且在状态发生改变时，拥有一个简单的回调函数的功能。

    下面是所有特性：

    - 保证设置新的标志或移除旧的标志的线程安全
    - 提供一种存储、管理一组标志的机制
    - 提供标志发生变化时回调已注册的可调用对象的能力
    - 继承此类，定义 Flag 类型的类成员以扩充支持的标志
    - 外部在某种条件成立或不成立时等待
    - 可定义互斥标志组

    """

    cls_emitter = BaseEventEmitter()

    _event_ = EventFactory(cls_emitter)
    # 正常状态下
    # cls obj 上只有一个 emitter，有若干个与之关联的 Event，每个 Event 的名称应该不相同，如果相同将视为同一事件
    # 对于这个 cls obj 的构造的每一个实例，都应该有一个自己的 emitter，
    # 且有与 cls obj 相应的 Event，但每个 Event 除了关联自己这个实例上的 emitter 外，还与 cls obj 上的 emitter 相关联
    event_set = _event_("set")
    event_unset = _event_("unset")

    def __init__(
        self,
        flags: Optional[_flags_type] = None,
        emitter: Optional[BaseEventEmitter] = None,
    ):
        super().__init__(emitter)

        self._cond = Condition()
        self._flags: Set[Flag] = set()

        if flags is not None:
            self.set(flags)

    def __str__(self):
        with self._cond:
            return f'{"|".join(f"{flag!s}" for flag in self._flags)}'

    def __repr__(self):
        return f"<{self.__class__.__name__} [{str(self)}]>"

    def __eq__(self, other):
        return self.equals(other, strict=False)

    def __op__(
        self, other: "FlagGroup", op: Callable[[Set[Flag], Set[Flag]], "FlagGroup"]
    ):
        with self._cond:
            return FlagGroup(op(self._flags, other._flags))

    def __or__(self, other: "FlagGroup"):
        return self.__op__(other, or_)

    def __and__(self, other: "FlagGroup"):
        return self.__op__(other, and_)

    def __xor__(self, other: "FlagGroup"):
        return self.__op__(other, xor)

    def __len__(self):
        with self._cond:
            return len(self._flags)

    def __iter__(self):
        return iter(self._flags)

    def __contains__(self, flag: Union[str, Flag]):
        return self.any([flag])

    def equals(self, other, *, strict=True):
        """
        判断此标志组与指定对象是否相等

        :param other: 指定对象
        :param strict: 是否启用严格模式，如果启用，则除了比较两者的 flags 是否一致之外，还会检查两者的 emitter 是否相同
        :return:
        """
        if isinstance(other, FlagGroup):
            if strict:
                return self._flags == other._flags and self._emitter == other._emitter
            else:
                return self._flags == other._flags

        return False

    @classmethod
    def _check_flag_conflict(cls, flag: Flag):
        """检查是否有冲突"""
        for name in flag.names:
            for registered_flag in cls.__get_registered_flags():
                if name == registered_flag:
                    raise ValueError(
                        f"the name {name!r} in {flag!r} conflicts with {registered_flag!r}"
                    )

    @classmethod
    def _check_loop(cls, flag: Flag, flags: Iterable[Flag]):
        # 采取 DFS（深度优先算法），由于我们能确保原先的标志组一定不成环，所以只需要确定引入新注册的标志们是否会导致成环
        visit_states = DefaultDict[Flag, bool](default_factory=bool)
        loop = []
        loop_nodes_collecting = False

        def visit(flag_: Flag):
            nonlocal loop_nodes_collecting

            visit_states[flag_] = True

            for parent in flag_.parents:
                parent_flag = cls.get_flag(parent, flags)

                if not visit_states.get(parent_flag, False):
                    visit(parent_flag)
                else:
                    loop.append(parent_flag)
                    loop_nodes_collecting = True
                    break

            if loop and flag_ in loop:
                loop_nodes_collecting = False
            if loop_nodes_collecting:
                loop.append(flag_)

        visit(flag)

        if loop:
            raise ValueError(
                f"{flag!r} will cause a circular reference problem"
                f" ({'<-'.join(str(flag) for flag in loop)}<-{loop[0]})"
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
        """
        注册新的 Flag 实例

        建议使用继承 FlagGroup 的方式来扩充支持的标志，而不是调用此方法直接注册

        :param flags: 要注册的标志实例
        :type flags: Iterable[Flag]
        :raises ValueError: 标志实例缺少可用名称时抛出
        """
        # 检查父名称是否有效
        cls._check_parent(flags)

        for flag in flags:
            if not flag.names:
                raise ValueError(f"{flag!r} must have at least one name or alias")

            # 检查名称和别名是否冲突
            cls._check_flag_conflict(flag)
            # 检查新加入该标志是否会导致环状引用问题
            cls._check_loop(flag, flags)
            # 将要注册的 flag 对象加入到当前已注册的标志中
            # NOTE: 这里不需要加锁，Python 的容器类型是线程安全的，何况对于 set 而言，重复添加并没有什么关系
            flags_ = cls.__get_this_cls_registered_flags()
            flags_.add(flag)

    def _wait(self, flag: Union[str, Flag], timeout=None, reverse: bool = False):
        predicate = lambda: reverse == self.has(flag)  # 实际上是异或

        with self._cond:
            return self._cond.wait_for(predicate, timeout=timeout)

    def wait(self, flag: Union[str, Flag], timeout=None):
        return self._wait(flag, timeout=timeout, reverse=False)

    def no_wait(self, flag: Union[str, Flag], timeout=None):
        return self._wait(flag, timeout=timeout, reverse=True)

    @contextmanager
    def _work(self):
        with self._cond:
            backup = self._flags.copy()
        try:
            yield self
        except Exception as e:
            with self._cond:
                self._flags = backup
            raise e

    def copy(self) -> "FlagGroup":
        with self._cond:
            return FlagGroup(self._flags)

    def any(self, flags: Iterable[Union[str, Flag]]):
        """
        判断指定的 flags 可迭代参数是否有任何一个标志在此标志组中，如果是则返回 True，否则返回 False

        :param flags: 指定一个可迭代的对象
        :return:
        """
        flags = self.to_flag_objs(flags)
        with self._cond:
            return any(flag in self._flags for flag in flags)

    def all(self, flags: Iterable[Union[str, Flag]], *, strict=False):
        """
        判断指定的 flags 可迭代参数中所有的标志是否都在此标志组中，如果是则返回 True，否则返回 False

        :param flags: 指定一个可迭代的对象
        :param strict: 是否启用严格模式，严格模式下，当且仅当指定的可迭代对象均在此标志组中且此标志组中的标志均在指定的可迭代对象中是返回真
        :return:
        """
        flags = self.to_flag_objs(flags)
        with self._cond:
            result = all(flag in self._flags for flag in flags)

            if not strict:
                return result

            return result and all(flag in flags for flag in self._flags)

    @classmethod
    def get_flag(
        cls, name: str, fallback_flags: Optional[Iterable[Flag]] = None
    ) -> Flag:
        """
        以指定名称寻找已注册的 `Flag` 对象

        默认的行为是从当前类对象中寻找符合指定名称的 `Flag` 对象，
        如果未找到且提供了 `fallback_flags` 参数，则尝试遍历该参数寻找符合指定名称的 `Flag` 对象,
        如果仍未找到，抛出 `ValueError`

        :param name: 要寻找的 Flag 对象名称，可以是别名
        :param fallback_flags: 备用 Flags, defaults to None
        :raises TypeErrorEx: 传入的 name 参数不为 str 类型
        :raises ValueError: 当前类对象中没有符合指定名称的 `Flag` 对象，且即使提供了 `fallback_flags`，也没有找到符合的 `Flag` 对象时抛出
        :return: `Flag` 对象
        """
        if not isinstance(name, str):
            raise TypeErrorEx(str, name, "name")

        name = name.strip()

        result = next(
            (flag for flag in cls.__get_registered_flags() if name in flag.names), None,
        )

        if result is not None:
            return result

        if fallback_flags is not None:
            result = next((flag for flag in fallback_flags if name in flag.names), None)

        if result is not None:
            return result

        raise ValueError(f"unknown name: {name!r}")

    @property
    def flags(self):
        """获取此标志组管理的标志对象集合副本"""
        with self._cond:
            return self._flags.copy()

    def has(self, flag: Union[str, Flag]):
        return self.__contains__(flag)

    @classmethod
    def to_flag_objs(cls, flags: _flags_type):
        if isinstance(flags, (str, Flag)):
            flags = [flags]
        elif not is_iterable(flags):
            raise TypeErrorEx((str, Flag, Iterable[Union[Flag, str]]), flags, "flags")

        result_set = set()
        # 遍历一遍以收集所有要设置的标志对象
        for flag in flags:
            if isinstance(flag, str):
                result_set.update(
                    cls.get_flag(single_flag_name)
                    for single_flag_name in flag.split("|")
                )
            elif isinstance(flag, Flag):
                # 检查兼容性
                if flag not in cls.__get_registered_flags():
                    raise ValueError(f"{flag!r} is not compatible with {cls!r}")
                result_set.add(flag)
            else:
                raise TypeErrorEx((str, Flag), flag)

        return result_set

    def set(self, flags: _flags_type, set_parent_flag_automatically=True):
        """
        设置新标志到此标志组中

        以下几种情况均会抛出 ``ValueError``：

        - 当尝试设置一个此标志组继承链上没有注册的标志名时：unknown name
        - 当尝试设置一个此标志组继承链上没有注册的标志对象时：not compatible
        - 当 set_parent_flag_automatically 为 False，而此标志组中缺少某个标志的父标志时：not exist
        - 当所设置的标志与互斥规则冲突时：cannot coexist

        :param flags: 要设置的标志
        :param set_parent_flag_automatically: 是否尝试为缺少父标志的标志设置相应的父标志，默认为 True
        """
        with self._work():  # 支持回滚
            # 遍历设置每一个收集到的标志对象
            for flag in self.to_flag_objs(flags):
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
                # 获取锁之后加入新标志，并通知所有等待状态改变的线程
                with self._cond:
                    self._flags.add(flag)
                    self._cond.notify_all()
                    self._emitter.emit("set", self, flag)

    def unset(self, flags: _flags_type, remove_all_children=True):
        """
        将某些标志从此标志组移除

        注意：如果所移除的标志有父标志，且父标志存在于当前标志组中，须先 `unset` 依赖于父标志的所有子标志，此功能未实现

        :param flags: 要移除的标志
        :param remove_all_children: 如果为 True，在移除标志时，会先递归移除依赖自己的子标志，再移除自己，默认为 True
        """
        with self._work():  # 支持回滚
            # 遍历取消设置每一个收集到的标志对象
            for flag in self.to_flag_objs(flags):
                # 检查要移除的标志的所有子标志，如果有子标志，且 remove_all_children 参数为 True, 则移除它，否则抛出异常
                for other_flag in self._flags - {flag}:
                    for other_flag_parent_name in other_flag.parents:
                        if other_flag_parent_name in flag.names:
                            if remove_all_children:
                                self.unset(
                                    other_flag, remove_all_children=remove_all_children
                                )
                            else:
                                raise ValueError(f"{other_flag} depends on {flag}")
                # 获取锁之后移除相应标志，并通知所有等待状态改变的线程
                with self._cond:
                    if flag in self._flags:  # 允许取消设置一个已经未设置的标志
                        self._flags.remove(flag)
                        self._emitter.emit("unset", self, flag)

                    self._cond.notify_all()

    def _get_mutex_groups(self):
        return []

    def _get_conflict_group(self, flags: Iterable[Flag]):
        for group in self._get_mutex_groups():
            mutex_group = set(group)
            conflict_group = mutex_group & set(flags)
            if len(conflict_group) > 1:
                return conflict_group

    @staticmethod
    def _get_registered_flags_attr_name(cls_):
        attr_base_name = f"__registered_flags"
        return f"_{cls_.__name__}{attr_base_name}"

    @classmethod
    def __get_registered_flags(cls) -> Generator[Flag, None, None]:
        for cls_ in getattr(cls, "__mro__"):
            cls_registered_flags = getattr(
                cls_, cls._get_registered_flags_attr_name(cls_), None
            )

            if cls_registered_flags:
                yield from cls_registered_flags

    @classmethod
    def __get_this_cls_registered_flags(cls):
        attr_name = cls._get_registered_flags_attr_name(cls)

        if not hasattr(cls, attr_name):
            setattr(cls, attr_name, set())

        return getattr(cls, attr_name)


class TaskFlagGroup(FlagGroup):
    """
    任务标志组
    --------
    提供了一些任务流程中常见的标志，并重写了互斥规则

    提供的标志
    ~~~~~~~~~ 

    - pending: 等待中
    - running: 运行中
    - stopping: 停止中
    - canceling: 取消中
    - stopping_with_exception: 因为异常而停止
    - stopping_with_canceled: 因为取消而停止

    互斥规则
    ~~~~~~~
    
    每一个规则之中的任何一个不能与此规则中其他标志同时存在

    - pending, running, stopping, canceling
    - stopping_with_exception, stopping_with_canceled

    父标志依赖
    ~~~~~~~~~

    - stopping_with_exception -> stopping
    - stopping_with_canceled -> stopping
    """

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
