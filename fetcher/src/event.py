#!/usr/env python3
from functools import partial
from typing import Optional

from pyee import BaseEventEmitter


class Event(object):
    """
    事件类
    ------
    本质上是借助一个或多个（继承于 BaseEventEmitter 的）触发器实现的，也因此可以以给定的事件名依次注册、订阅这些触发器
    """

    def __init__(self, name: str, *emitter: BaseEventEmitter):
        self._emitters = emitter
        self._name = name

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

    @property
    def name(self):
        return self._name

    @property
    def emitters(self):
        return self._emitters

    def on(self, func=None):
        return list(map(lambda emitter: emitter.on(self._name, func), self._emitters))

    def once(self, func=None):
        return list(map(lambda emitter: emitter.once(self._name, func), self._emitters))

    def emit(self, *args, **kwargs):
        return list(
            map(
                lambda emitter: emitter.emit(self._name, *args, **kwargs),
                self._emitters,
            )
        )

    def remove_listener(self, func=None):
        return list(
            map(
                lambda emitter: emitter.remove_listener(self._name, func),
                self._emitters,
            )
        )

    def remove_all_listeners(self):
        return list(
            map(
                lambda emitter: emitter.remove_all_listeners(self._name), self._emitters
            )
        )

    def listeners(self):
        return list(map(lambda emitter: emitter.listeners(self._name), self._emitters))


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


class EmitterMixin(object):
    def __init__(self, emitter: Optional[BaseEventEmitter] = None):
        emitters = []

        self._emitter = emitter or BaseEventEmitter()
        emitters.append(self._emitter)
        # 检查是否存在类级别的 emitter，如果存在则加入到 emitters 中
        if hasattr(self, "cls_emitter"):
            emitters.append(self.cls_emitter)

        event_factory = EventFactory(*emitters)

        for k, v in self.__dict__.copy().items():
            if isinstance(v, Event):
                setattr(self, k, event_factory(v.name))

    @property
    def emitter(self):
        return self._emitter
