#!/usr/env python3
import math
from abc import ABCMeta
from contextlib import contextmanager
from functools import partial
from inspect import signature
from itertools import count
from threading import Thread
from typing import Callable, Iterable, Optional, Union

from pyee import AsyncIOEventEmitter

from .util import jump_step

_handler_type = Union[Callable[[int, "BaseFetcher"], None], Callable[[int], None]]


class BaseFetcher(Thread, metaclass=ABCMeta):
    __counter = 0

    def __init__(self, name=None, emitter=None):
        self._working = False
        self._handlers = {}
        self.emitter = emitter or AsyncIOEventEmitter()

        super().__init__(name=name or f"{self.__class__.__name__}-{self.__auto_counter()}")

    @property
    def working(self):
        return self._working

    @contextmanager
    def _work(self):
        self._working = True
        try:
            yield
        finally:
            self._working = False

    def add_handler(self, handler: _handler_type = None, **kwargs):
        if handler is None:
            return partial(self.add_handler, **kwargs)

        handler_sig = signature(handler)

        self._handlers[handler] = handler_sig.parameters

        return handler

    def remove_handler(self, handler: _handler_type):
        self._handlers.pop(handler, None)

    def clear_handlers(self):
        self._handlers.clear()

    def __auto_counter(self):
        counter = self.__counter
        counter += 1
        return counter


class IndexFetcher(BaseFetcher):

    def __init__(self, begin: int, end: Optional[int] = None, step: int = 1,
                 jump_step_func: Callable[[], Iterable[int]] = None, jump_step_limit=3, name=None, emitter=None):
        if end is None:
            end = math.copysign(float("inf"), step)

        if (begin < end and step <= 0) or (begin > end and step >= 0):
            raise ValueError(
                f"The value of step ({step}) is not ensure to reach the end.")

        self.begin = begin
        self.end = end
        self.step = step
        self.jump_step_func = jump_step_func or jump_step
        self.jump_step_limit = jump_step_limit

        self._current = None
        self._worked_span = (None, None)

        super().__init__(name=name, emitter=emitter)

    def __eq__(self, other: "IndexFetcher"):
        if not isinstance(other, IndexFetcher):
            return False
        return (self.begin, self.end, self.step) == (other.begin, other.end, other.step)

    def __hash__(self):
        return hash(self.begin) ^ hash(self.end) ^ hash(self.step)

    def __repr__(self):
        return f"<IndexSpider span=({self.span[0]} ~ {self.span[1]}) working={self.working}" \
               f" current={self.current}" \
               f" step={self.step}" \
               f" worked_span=({self.worked_span[0]} ~ {self.worked_span[1]})" \
               f" jump_step_limit={self.jump_step_limit}>"

    def __str__(self):
        working_flag = "*" if self.working else ""
        current_field = f" at {self.current}" if self.current else ""

        return f"{working_flag}IndexSpider({self.span[0]} ~ {self.span[1]}, " \
               f"{float(self.process * 100) :.2F}%){current_field}"

    @property
    def current(self):
        return self._current

    @property
    def span(self):
        return (self.begin, self.end) if self.begin <= self.end else (self.end, self.begin)

    @property
    def worked_span(self):
        return self._worked_span

    @property
    def process(self):
        """获取当前工作进度（0~1 之间的浮点数表示）"""
        if self._worked_span[0] is None or self._worked_span[1] is None:
            return 0
        return (self._worked_span[1] - self._worked_span[0]) / (self.span[1] - self.span[0])

    def add_handler(self, handler: _handler_type = None, **kwargs):
        if handler is None:
            return partial(self.add_handler, **kwargs)

        handler_sig = signature(handler)
        if len(handler_sig.parameters) not in (1, 2):
            raise ValueError("handler's signature should have at least one parameter, "
                             "less than or equal to two parameters.")
        super().add_handler(handler, **kwargs)

    def reverse(self):
        self.step = - self.step
        return self

    def run(self):
        self._worked_span = (None, None)
        with self._work():
            try:
                self.emitter.emit("starting", self)
                self._normal()
            except (IndexError, OverflowError) as e:
                self._current = None
                self._worked_span = self.span
                self.emitter.emit("exiting", self, e)

    @contextmanager
    def _anchor(self):
        current = self.current
        try:
            yield current
        finally:
            self._set_current(current)

    @contextmanager
    def list(self, handler: _handler_type = None, only_index=True, record_valid_data=True):
        result = []

        @self.add_handler
        def _collector_(i, fetcher_):
            if not record_valid_data:
                result.append(i if only_index else (i, fetcher_))
            handler and handler(i)
            if record_valid_data:
                result.append(i if only_index else (i, fetcher_))

        # 启动 fetcher，等待以搜集全部结果
        self.start()
        self.join()
        yield result
        self.remove_handler(_collector_)

    def _normal(self, begin=None, step=None, rejected_handler: Optional[Callable[[int], Optional[bool]]] = None,
                prioritized_break_condition: Optional[Callable[[int], bool]] = None):

        begin = begin or self.begin
        step = step or self.step
        rejected_handler = rejected_handler or (
            lambda i_: self._jump(i_) or True)

        if step == 0:
            self._set_current(begin)
            self.__safe_handle()
        else:
            for i in count(begin, step):
                self._set_current(i)
                if prioritized_break_condition and prioritized_break_condition(i):
                    break
                if not self.__safe_handle():
                    if rejected_handler(i):
                        break

    def _jumper(self, begin, sign):
        i = begin
        for d in self.jump_step_func():
            i += int(math.copysign(d, sign))
            yield i

    def _back(self, last_unacceptable_value):
        def overdo(i):
            # 新迭代出的值越过了跃进时最后一次未被接受的值
            return (-self.step < 0 and i <= last_unacceptable_value) \
                   or (-self.step > 0 and i >= last_unacceptable_value)

        # 回溯一下，保证尽量不遗漏
        with self._anchor() as cur:
            # 回溯之前也需要通过反向跃进跳过不被接受的索引值
            for counter, j in enumerate(self._jumper(cur, -self.step)):
                # 到达限制跳出循环
                if counter >= self.jump_step_limit:
                    return
                # 新迭代出的值越过了跃进时最后一次未被接受的值
                if overdo(j):
                    return
                self._set_current(j)
                # 出现能接受的索引值，开始回溯
                if self.__safe_handle():
                    # 开始回溯操作，再次被拒绝则不再处理
                    self._normal(self.current - self.step, -self.step,
                                 rejected_handler=lambda i_: True,
                                 prioritized_break_condition=lambda i_: overdo(i_))
                    return

    def _jump(self, begin):
        last_unacceptable_value = begin
        # 按 self.step 的符号方向跃进
        for i in self._jumper(begin, self.step):
            self._set_current(i)
            if self.__safe_handle():
                # 处理成功，需要回溯一下，保证尽量不遗漏
                self._back(last_unacceptable_value)
                # 回溯完毕，停止跃进
                break
            else:
                # 如果未处理成功，继续跃进
                last_unacceptable_value = i

        # 从当前索引+步进偏移 恢复普通步进状态
        self._normal(self.current + self.step)

    def __safe_handle(self):
        try:
            self.emitter.emit("handling", self)
            self._handle()
            self.emitter.emit("handled", self)
            return True
        except Exception as e:
            self.emitter.emit("handle_error", self, e)
            return False

    def _handle(self):
        for handler, params in self._handlers.items():
            if len(params) == 1:
                handler(self.current)
            elif len(params) == 2:
                handler(self.current, self)

    def _in_span(self, i):
        span_l, span_r = self._worked_span
        if span_l is None or span_r is None:
            return False
        return span_l <= i <= span_r

    def _set_span(self, i):
        if i < self.span[0] or self.span[1] < i:
            raise IndexError(i)

        span_l, span_r = self._worked_span
        if span_l is None or span_r is None:
            self._worked_span = (i, i)
        else:
            self._worked_span = (min(span_l, i), max(i, span_r))

    def _set_current(self, i):
        i = int(i)
        self._set_span(i)
        self._current = i
