#!/usr/env python3
import math
from contextlib import contextmanager
from functools import partial
from inspect import signature
from itertools import count
from typing import Callable, Iterable, Optional, Union

from pyee import AsyncIOEventEmitter

from .exceptions import JobCancelError, RefuseHandleError
from .span import WorkSpan
from .util import jump_step, repr_injector

_handler_type = Union[Callable[[int, "BaseJob"], None], Callable[[int], None]]


class Handlers(dict):
    def add(self, handler: _handler_type = None, **kwargs):
        if handler is None:
            return partial(self.add, **kwargs)

        handler_sig = signature(handler)

        self[handler] = handler_sig.parameters

        return handler


class BaseJob(object):
    def __init__(self, emitter=None):
        self.handlers = Handlers()
        self.emitter = emitter or AsyncIOEventEmitter()

        self._working = False
        self._cancel = False

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

    def _try_cancel(self):
        if self._cancel:
            raise JobCancelError

    def cancel(self):
        self._cancel = True


@repr_injector
class IndexJob(BaseJob, WorkSpan):
    def __init__(self, begin: int, end: Optional[int] = None, step: int = 1,
                 jump_step_func: Callable[[], Iterable[int]] = None, jump_step_limit=3, emitter=None):

        self.jump_step_func = jump_step_func or jump_step
        self.jump_step_limit = jump_step_limit

        BaseJob.__init__(self, emitter=emitter)
        WorkSpan.__init__(self, begin, end, step)

    def __call__(self, *args, **kwargs):
        return self.run()

    def run(self):
        self._worked_span = None
        with self._work():
            try:
                self.emitter.emit("starting", self)
                self._normal()
            except (IndexError, OverflowError, RefuseHandleError, JobCancelError) as e:
                self._current = None
                self._worked_span = self
                self.emitter.emit("exiting", self, e)

    @contextmanager
    def list(self, handler: _handler_type = None, only_index=True, record_valid_data=True):
        result = []

        @self.handlers.add
        def _collector_(i, fetcher_):
            if not record_valid_data:
                result.append(i if only_index else (i, fetcher_))
            handler and handler(i)
            if record_valid_data:
                result.append(i if only_index else (i, fetcher_))

        self.run()
        yield result
        self.handlers.pop(_collector_)

    @contextmanager
    def _anchor(self):
        current = self.current
        try:
            yield current
        finally:
            self._set_current(current)

    def _jumper(self, begin, sign):
        i = begin
        for d in self.jump_step_func():
            i += int(math.copysign(d, sign))
            yield i

    def _normal(self, begin=None, step=None, rejected_handler: Optional[Callable[[int], Optional[bool]]] = None,
                prioritized_break_condition: Optional[Callable[[int], bool]] = None):
        begin = begin or self.begin
        step = step or self.step
        rejected_handler = rejected_handler or (lambda i_: self._jump(i_) or True)

        if step == 0:
            self._set_current(begin)
            self.__safe_handle()
        else:
            for i in count(begin, step):
                self._try_cancel()
                self._set_current(i)
                if prioritized_break_condition and prioritized_break_condition(i):
                    break
                if not self.__safe_handle():
                    if rejected_handler(i):
                        break

    def _back(self, first_unaccepted_value):
        def overdo(i):
            # 新迭代出的值越过了跃进时首次未被接受的值
            return (-self.step < 0 and i <= first_unaccepted_value) \
                   or (-self.step > 0 and i >= first_unaccepted_value)

        # 回溯一下，保证尽量不遗漏
        with self._anchor() as cur:
            # 回溯之前也需要通过反向跃进跳过不被接受的索引值
            for counter, j in enumerate(self._jumper(cur, -self.step)):
                self._try_cancel()
                # 到达限制跳出循环
                if counter >= self.jump_step_limit:
                    return
                # 新迭代出的值越过了跃进时首次未被接受的值
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
        # 按 self.step 的符号方向跃进
        for i in self._jumper(begin, self.step):
            self._try_cancel()
            self._set_current(i)
            if self.__safe_handle():
                # 处理成功，需要回溯一下，保证尽量不遗漏
                self._back(begin)
                # 回溯完毕，停止跃进
                break

        # 从当前索引+步进偏移 恢复普通步进状态
        self._normal(self.current + self.step)

    def __safe_handle(self):
        try:
            self.emitter.emit("handling", self)
            self._handle()
            self.emitter.emit("handled", self)
            return True
        except RefuseHandleError as e:
            raise e
        except Exception as e:
            self.emitter.emit("handle_error", self, e)
            return False

    def _handle(self):
        for handler, params in self.handlers.items():
            if len(params) == 1:
                handler(self.current)
            elif len(params) == 2:
                handler(self.current, self)
            else:
                raise ValueError(f"Unsupported handler: {handler}")

    def _set_current(self, i):
        i = int(i)
        self._update_worked_span(i)
        self._current = i
