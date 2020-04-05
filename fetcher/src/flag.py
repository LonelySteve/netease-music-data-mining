#!/usr/env python3
from contextlib import contextmanager
from functools import partial
from itertools import chain
from threading import RLock
from typing import (
    DefaultDict,
    FrozenSet,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)

from .exceptions import TypeErrorEx
from .utils import is_iterable, repr_injector, void


@repr_injector
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

    支持使用字符串与 Flag 实例进行比较：

    >>> flag = Flag(name="aaa", aliases="bbb|ccc")
    >>> assert flag == "aaa"
    >>> assert flag == "bbb" and flag == "ccc"

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
    -------
    标志组主要是一种存储、管理一组标志（Flag 类的实例）的容器，并且在状态发生改变时，拥有一个简单的回调函数的功能。

    下面是所有特性：

    - 保证设置新的标志或移除旧的标志的线程安全
    - 提供一种存储、管理一组标志的机制
    - 提供标志发生变化时回调已注册的可调用对象的能力
    - 继承此类，定义 Flag 类型的类成员以扩充支持的标志
    - 可定义互斥标志组
    """

    _LOCK = RLock()

    def __init__(self, flags: Optional[_flags_type] = None):
        self._flags = set()
        self._callbacks: DefaultDict[Flag, List[FlagGroupCallbackInfo]] = DefaultDict[
            Flag, List[FlagGroupCallbackInfo]
        ](list)

        if flags:
            self.set(flags)

    def __str__(self):
        return f'[{"|".join(f"{flag!s}" for flag in self._flags)}]'

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
            # NOTE: 这里不需要加锁，Python 的容器类型是线程安全的，何况对于 set 而言，出现重复并没有什么关系
            flags_ = cls.__get_this_cls_registered_flags()
            flags_.add(flag)

    @contextmanager
    def _work(self):
        backup = self._flags.copy()
        try:
            yield self
        except Exception as e:
            with self._LOCK:
                self._flags = backup
            raise e

    def copy(self) -> "FlagGroup":
        return FlagGroup(*self)

    def any(self, flags: Iterable[Union[str, Flag]]):
        return bool(next((flag for flag in flags if flag in self), None))

    def all(self, flags: Iterable[Union[str, Flag]]):
        return all((flag in self) for flag in flags)

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
                result_set.update(
                    self.get_flag(single_flag_name)
                    for single_flag_name in flag.split("|")
                )
            elif isinstance(flag, Flag):
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
                # 检查兼容性
                if flag not in self.__get_registered_flags():
                    raise ValueError(f"{flag!r} is not compatible with {self!r}")
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
                with self._LOCK:
                    self._flags.add(flag)

    def unset(self, flags: _flags_type, remove_parent_flags_all_children=False):
        """
        将某些标志从此标志组移除

        注意：如果所移除的标志有父标志，且父标志存在于当前标志组中，须先 `unset` 依赖于父标志的所有子标志，此功能未实现
        
        :param flags: 要移除的标志
        :param remove_parent_flags_all_children: 如果为 True，在移除带有父标志的标志时，会先移除依赖于父标志的其他子标志，再移除父标志，最后移除自己，默认为 False
        """
        with self._work():  # 支持回滚
            # 遍历取消设置每一个收集到的标志对象
            for flag in self.to_flag_objs(flags):
                # 检查要移除的标志是否被依赖
                for flag_ in self._flags - {flag}:
                    if flag in flag_.parents:
                        raise ValueError(f"{flag!r} is dependent on {flag_!r}")
                # RLock 获取锁之后移除标志
                with self._LOCK:
                    if flag in self._flags:  # 允许取消设置一个已经未设置的标志
                        self._flags.remove(flag)

    def emit(self, flag: Union[str, Flag], is_set: bool, *args, **kwargs):
        """
        触发设置或取消设置标志的动作，然后尝试使用传入的可变参数回调已注册的可调用对象
        
        :param flag: 要设置或取消设置的标志
        :param is_set: 如果为 True，则设置 ``flag`` 参数指定的标志，否则取消设置该标志
        """
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

    def set_emit(self, flag: Union[str, Flag], *args, **kwargs):
        """
        触发设置标志的动作，然后尝试使用传入的可变参数回调已注册的可调用对象
        
        :param flag: 要设置的标志
        """
        self.emit(flag, True, *args, **kwargs)

    def unset_emit(self, flag: Union[str, Flag], *args, **kwargs):
        """
        触发取消设置标志的动作，然后尝试使用传入的可变参数回调已注册的可调用对象
        
        :param flag: 要取消设置的标志
        """
        self.emit(flag, False, *args, **kwargs)

    def when(
        self,
        flag: Union[str, Flag],
        callback=None,
        *,
        is_set: Optional[bool] = None,
        max_call_count=-1,
    ):
        """
        注册回调函数
        
        :param flag: 要绑定的标志
        :param callback: 回调函数，默认为 None
        :param is_set: 标志的触发状态，为 True 是设置时触发，为 False 是取消设置时触发，为 None 则发生前两者动作时都会触发，默认为 None
        :param max_call_count: 最大调用次数，为 -1 （或为负数时）时不限制，默认为 -1
        """
        if callback is None:
            return partial(
                self.when, flag, is_set=is_set, max_call_count=max_call_count
            )

        if not isinstance(max_call_count, int):
            raise TypeErrorEx(int, max_call_count, "max_call_count")
        if isinstance(flag, str):
            flag = self.get_flag(flag)
        if not isinstance(flag, Flag):
            raise TypeErrorEx((str, Flag), flag, " flag")

        self._callbacks[flag].append(
            FlagGroupCallbackInfo(
                callback, is_set=is_set, max_call_count=max_call_count
            )
        )

        return callback

    def on_set(self, flag: Union[str, Flag], callback=None):
        return self.when(flag, is_set=True, callback=callback)

    def once_set(self, flag: Union[str, Flag], callback=None):
        return self.when(flag, is_set=True, max_call_count=1, callback=callback)

    def on_unset(self, flag: Union[str, Flag], callback=None):
        return self.when(flag, is_set=False, callback=callback)

    def once_unset(self, flag: Union[str, Flag], callback=None):
        return self.when(flag, is_set=False, max_call_count=1, callback=callback)

    def on(self, flag: Union[str, Flag], callback=None):
        return self.when(flag, callback=callback)

    def once(self, flag: Union[str, Flag], callback=None):
        return self.when(flag, callback=callback, max_call_count=1)

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
