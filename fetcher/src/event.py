#!/usr/env python3
from functools import partial
from types import DynamicClassAttribute
from typing import Optional, Callable

from pyee import BaseEventEmitter
from enum import unique, Enum

from src.exceptions import TypeErrorEx


@unique
class EventPropagationLevel(Enum):
    instance = 0
    class_object = 1
    base_class_object = 2


class PropagationLevelControlMixin(object):
    def __init__(self):
        self._level: Optional[EventPropagationLevel] = None

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, value):
        if value is not None and not isinstance(value, EventPropagationLevel):
            raise TypeErrorEx((EventPropagationLevel, type(None)), value, "value")

        self._level = value


class EventControlBlock(PropagationLevelControlMixin):
    def __init__(self, class_emitter, bases):
        super().__init__()

        self.bases = bases
        self.class_emitter = class_emitter


class InstanceEventControlBlock(EventControlBlock):
    def __init__(self, class_emitter, bases, instance_emitter):
        super().__init__(class_emitter, bases)

        self.instance_emitter = instance_emitter


class Event(PropagationLevelControlMixin):
    """
    事件类
    ------
    本质上是借助一个或多个（继承于 BaseEventEmitter 的）触发器实现的，也因此可以以给定的事件名依次注册、订阅这些触发器
    """

    def __init__(self, name: str, control_block: Optional[EventControlBlock] = None):
        super().__init__()

        self._name = name
        self._control_block = control_block
        self._level: Optional[
            EventPropagationLevel
        ] = EventPropagationLevel.base_class_object

    def __add__(self, other):
        self.on(other)
        return self

    def __sub__(self, other):
        self.remove_listener(other)
        return self

    def __call__(self, func=None, *, once=False):
        if func is None:
            return partial(self.__call__, once=once)

        if once:
            return self.once(func)

        return self.on(func)

    def _get_level_value(self):
        # 优先使用 Event 对象上的 level，如果 Event 对象的 level 为空，则使用 ControlBlock 里的 level，如果仍然为空，则取最大值
        if self._level is not None:
            return self._level.value
        if self._control_block.level is not None:
            return self._control_block.level.value

        return max(member.value for member in EventPropagationLevel)

    def emitters_iter(self):
        """
        获取当前上下文可用的触发器，这是一个生成器

        :return:
        """
        level_value = self._get_level_value()

        if level_value >= EventPropagationLevel.instance.value:
            if isinstance(self._control_block, InstanceEventControlBlock):
                yield self._control_block.instance_emitter

        if level_value >= EventPropagationLevel.class_object.value:
            yield self._control_block.class_emitter

        if level_value >= EventPropagationLevel.base_class_object.value:
            # 遍历每一个基类，寻找有 cls_emitter 属性的基类，迭代它所属的触发器
            for base in self._control_block.bases:
                if hasattr(base, "cls_emitter"):
                    yield base.cls_emitter

    @property
    def emitters(self):
        """
        获取当前上下文可用的触发器，以列表形式返回

        :return:
        """
        return list(self.emitters_iter())

    @property
    def name(self):
        return self._name

    @property
    def control_block(self):
        return self._control_block

    def on(self, func=None):
        return list(
            map(lambda emitter: emitter.on(self._name, func), self.emitters_iter(),)
        )

    def once(self, func=None):
        return list(
            map(lambda emitter: emitter.once(self._name, func), self.emitters_iter(),)
        )

    def emit(self, sender, *args, **kwargs):
        return list(
            map(
                lambda emitter: emitter.emit(self._name, sender, *args, **kwargs),
                self.emitters_iter(),
            )
        )

    def remove_listener(self, func=None):
        return list(
            map(
                lambda emitter: emitter.remove_listener(self._name, func),
                self.emitters_iter(),
            )
        )

    def remove_all_listeners(self):
        return list(
            map(
                lambda emitter: emitter.remove_all_listeners(self._name),
                self.emitters_iter(),
            )
        )

    def listeners(self):
        return list(
            map(lambda emitter: emitter.listeners(self._name), self.emitters_iter(),)
        )


class EventFactory(object):
    """
    事件工厂
    -------
    拥有生产共用触发器的事件的能力
    """

    def __init__(self, *emitter):
        self._emitters = emitter

    def __call__(self, name: str) -> Event:
        return Event(name, *self._emitters)


class EventSystemMeta(type):
    """
    事件系统元类
    """

    def __new__(
        mcs,
        name,
        bases,
        attrs,
        *,
        emitter_factory: Optional[Callable[[], BaseEventEmitter]] = None
    ):
        _emitter_factory = emitter_factory or (lambda: BaseEventEmitter())

        _event_dict = {}  # 临时保存 属性名-事件名
        # 优先从 class 字典的 __annotations__ 中寻找 Event 类型的量
        annotations = attrs.get("__annotations__", {})
        for k, v in annotations.items():
            if issubclass(v, Event):
                _event_dict[k] = k
        # 收集类中定义的 Event
        for k, v in attrs.items():
            if isinstance(v, Event):
                _event_dict[k] = v.name
        # 注入一些类实例化时用得着的量
        attrs["_emitter_factory"] = _emitter_factory
        attrs["_event_dict"] = _event_dict
        attrs["_bases"] = bases
        # hack 注入「新鲜」的 emitter
        cls_emitter = attrs["cls_emitter"] = _emitter_factory()
        # 构造事件控制块对象
        event_control_block = attrs["_event_control_block"] = EventControlBlock(
            cls_emitter, bases
        )
        # 为类对象重新生成 Event 对象
        for attr_name, event_name in _event_dict.items():
            attrs[attr_name] = Event(event_name, event_control_block)

        return super().__new__(mcs, name, bases, attrs)

    def __init__(
        cls,
        name,
        bases,
        attrs,
        *,
        emitter_factory: Optional[Callable[[], BaseEventEmitter]] = None
    ):
        # Do nothing but can't omit
        super().__init__(name, bases, attrs)

    def __getattr__(self, item):
        if item == "event_control_block":
            return self._event_control_block

        return super().__getattribute__(item)

    def __setattr__(self, key, value):
        # hack: 要保证设置的如果是 Event 对象，就需要对其进行替换，以保证它正常工作
        # TODO: 考虑下原来可能存在的 Event 对象的感受？
        if isinstance(value, Event):
            value = Event(
                value.name,
                control_block=EventControlBlock(
                    getattr(self, "cls_emitter"), getattr(self, "_bases")
                ),
            )

        super().__setattr__(key, value)


class EventSystem(metaclass=EventSystemMeta):
    """
    事件系统
    -------
    此类为可以为被继承的子类提供事件注册、回调能力。

    被继承的子类，包括被继承的子类的子类（依次类推），都拥有独立的触发器，它们这些类的实例也拥有独立的触发器。

    实例和相应的类的触发器是有关联的，实例上被触发的事件同样也会触发其类上相同的事件，换句话说，对于类上的事件回调函数，
    会在此类的所有实例的相应事件被触发时调用。而对于实例上的事件回调函数只会被该实例的相应事件被触发时被调用。

    这意味着调用方可以决定将事件回调绑定在类上还是类构造的实例上，如果事件回调被绑定到类构造的实例上，事实上它也会被绑定到该实例相应的类上

    调用的顺序是：实例上的事件回调函数（如果有） -> 对应类上的事件回调函数（如果有）

    一个简单的使用例子见下方
    ::
        >>> class DemoClass(EventSystem):
        >>>     event_loading: Event
        >>>     # 如果希望内部使用的事件名称与属性定义名不相同的话，可以使用构造 Event 对象并赋值的方法，
        >>>     # 但如果你想通过这种方法指定 emitter 的话，是不行的（
        >>>     event_closing = Event(name="closing")

        >>> @DemoClass.event_loading.on
        >>> def class_level_callback(sender, *args, **kwargs):
        >>>    print("class_level_callback")

        >>> demo_obj = DemoClass()

        >>> @demo_obj.event_loading.on
        >>> def object_level_callback(sender, *args, **kwargs):
        >>>    print("object_level_callback")

        >>> demo_obj.event_loading.emit("for test")
        object_level_callback
        class_level_callback
    """

    def __init__(self):
        self.instance_emitter = getattr(self.__class__, "_emitter_factory")()
        # 构造实例事件控制块对象
        event_control_block = self._event_control_block = InstanceEventControlBlock(
            getattr(self.__class__, "cls_emitter"),
            getattr(self.__class__, "_bases"),
            self.instance_emitter,
        )
        # 为实例重新生成 Event 对象
        for attr_name, event_name in getattr(self.__class__, "_event_dict").items():
            setattr(self, attr_name, Event(event_name, event_control_block))

    # py 3.4 引入的新特性，使用此装饰器，使得通过类访问该属性与通过实例访问该属性的行为可以不相同
    # 通过实例访问此属性的行为仍然与普通的属性（Property）相同
    # 但通过类访问此属性会被路由到类的 __getattr__  方法（元类里的 __getattr__ 方法）
    @DynamicClassAttribute
    def event_control_block(self):
        """
        获取实例或类上的事件控制块

        如果想获取类上的事件控制块，请使用以下方式：

        >>> EventSystem.event_control_block

        如果想获取实例上的事件控制块，请使用以下方式：

        >>> EventSystem().event_control_block

        :return:
        """
        return self._event_control_block
