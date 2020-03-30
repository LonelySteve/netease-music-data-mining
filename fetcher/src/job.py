#!/usr/env python3
import math
import sys
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from copy import copy
from functools import partial
from inspect import signature
from itertools import count
from typing import Callable, Iterable, Optional, Union

from pyee import AsyncIOEventEmitter

from .exceptions import (ExplicitlySkipHandlingError,
                         ExplicitlyStopHandlingError, JobCancelError)
from .flag import JobStepFlag
from .span import StepSpan, WorkSpan
from .status import IStatus
from .util import jump_step, repr_injector

_handler_type = Union[Callable[[int, "BaseJob"], None], Callable[[int], None]]


class Handlers(object):
    def __init__(self, handlers=None):
        if isinstance(handlers, Handlers):
            self._data = handlers._data.copy()
        else:
            self._data = handlers or {}

    def add(self, handler: _handler_type = None, **kwargs):
        if handler is None:
            return partial(self.add, **kwargs)

        handler_sig = signature(handler)

        self._data[handler] = handler_sig.parameters

        return handler

    def items(self):
        return self._data.items()

    def clear(self):
        return self._data.clear()

    def pop(self, k):
        return self._data.pop(k)


@repr_injector
class BaseJob(IStatus, metaclass=ABCMeta):
    def __init__(self, emitter=None, flag: Optional[JobStepFlag] = None):
        self.handlers = Handlers()
        self._emitter = emitter or AsyncIOEventEmitter()
        self._flag: JobStepFlag = flag or JobStepFlag(JobStepFlag.pending)

    def __call__(self, *args, **kwargs):
        return self.run()

    @property
    def working(self):
        return JobStepFlag.running in self.flag

    @property
    def flag(self):
        return self._flag

    @abstractmethod
    def run(self):
        """运行任务，此方式是同步的"""

    @contextmanager
    def _work(self):
        # 检查是否已结束，如果已为结束状态则不可再次启动工作
        if JobStepFlag.stopping in self.flag:
            raise RuntimeError("Cannot start a stopped job again!")
        self._flag -= JobStepFlag.pending
        self._flag += JobStepFlag.running
        try:
            yield
        finally:
            # 先取消 running 标志，再置 stopping 标志后
            self._flag -= JobStepFlag.running
            self._flag += JobStepFlag.stopping

    def cancel(self):
        self._flag -= JobStepFlag.running
        self._flag += JobStepFlag.canceling

    def _try_cancel(self):
        if JobStepFlag.canceling in self.flag:
            raise JobCancelError


class IndexJob(BaseJob, WorkSpan):

    def __init__(self, begin: int, end: Optional[int] = None, step: int = 1,
                 jump_step_func: Callable[[], Iterable[int]] = None, emitter=None):

        self.jump_step_func = jump_step_func or jump_step
        self.job_span = StepSpan(begin, end, step)

        self._break_point_span: Optional[StepSpan] = None
        self._break_point_current: Optional[int] = None
        self._reverse_leaping_first_unaccepted_value: Optional[int] = None

        BaseJob.__init__(self, emitter=emitter)
        WorkSpan.__init__(self, begin, end, step)

    def __str__(self):
        return f"{self.__class__.__name__}({WorkSpan.__str__(self)})"

    def __eq__(self, other):
        if not isinstance(other, IndexJob):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self):
        # NOTE: 重写基类 WorkSpan 的 __hash__ 方法，我们希望这个类是按其在内存中的地址进行 hash 计算的，即基类 BaseJob 的算法
        return BaseJob.__hash__(self)

    @property
    def emitter(self):
        return self._emitter

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

    def run(self):
        self._worked_span = None
        with self._work():
            err_info = (None, None, None)
            # noinspection PyBroadException
            try:
                self._emitter.emit("IndexJob.running", self)
                self._current = self.job_span.begin
                self._flag += JobStepFlag.stepping
                # 循环处理，下面是步骤简化图：
                #
                # 步进 ---> 跃进
                #  ^ ^      |
                #  |  \     |
                #  |   \    |
                #  |    \   |
                #  |     \  v
                # 反向步进<--反向跃进
                #
                while True:
                    self._emitter.emit("IndexJob.step_switch", self)

                    if JobStepFlag.stepping in self.flag:
                        self._handle_stepping()
                    elif JobStepFlag.leaping in self.flag:
                        self._handle_leaping()
            except IndexError:
                err_info = sys.exc_info()
            except JobCancelError:
                err_info = sys.exc_info()
                self._flag -= JobStepFlag.canceling
                self._flag += JobStepFlag.stopping_with_canceled
            except AssertionError:
                err_info = sys.exc_info()
                raise err_info[2]  # for test
            except Exception:
                err_info = sys.exc_info()
                self._flag -= JobStepFlag.running
                self._flag += JobStepFlag.stopping_with_exception
            finally:
                self._break_point_span = None
                self._break_point_current = None
                self._reverse_leaping_first_unaccepted_value = None
                self._emitter.emit("IndexJob.stopped", self, err_info)

    def _jumper(self, begin, sign):
        i = begin
        for d in self.jump_step_func():
            i += int(math.copysign(d, sign))
            yield i

    def _step(self):
        if self.job_span.step == 0:
            self._set_current(self.job_span.begin)
            self.__safe_handle()
            return

        for i in count(self.current, self.job_span.step):
            self._try_cancel()
            self._set_current(i)
            if not self.__safe_handle():
                break

    def _leap(self):
        for i in self._jumper(self.current, self.job_span.step):
            self._try_cancel()
            self._set_current(i)
            if self.__safe_handle():
                break

    def _prepare_stepping(self):
        # ===============为「步进」状态做准备（「反向步进/反向跃进」->「步进」）====================
        # 取消反转标志位
        self._flag -= JobStepFlag.reverse
        # 取消有可能存在的跃进标志
        self._flag -= JobStepFlag.leaping
        # 设置步进状态标志位
        self._flag += JobStepFlag.stepping
        # NOTE 不需要手动反转以恢复方向，下面恢复断点的过程会重置方向
        # 恢复断点
        self.job_span = copy(self._break_point_span)
        self._current = self._break_point_current + self.job_span.step

    def _prepare_leaping(self):
        # ===============为「跃进」状态做准备（「步进」->「跃进」）====================
        self._reverse_leaping_first_unaccepted_value = self._current
        self._flag -= JobStepFlag.stepping
        self._flag += JobStepFlag.leaping

    def _prepare_reverse_stepping(self):
        # ===============为「反向步进」状态做准备（「反向跃进」->「反向步进」）====================
        self._flag -= JobStepFlag.leaping
        self._flag += JobStepFlag.stepping
        self._current += self.job_span.step

    def _prepare_reverse_leaping(self):
        # ===============为「反向跃进」状态做准备（「跃进」->「反向跃进」）====================
        # 保存断点
        self._break_point_span = copy(self.job_span)
        self._break_point_current = self._current

        # 步伐反向
        self.job_span.step = - self.job_span.step
        self.job_span.begin = self._current
        self.job_span.end = self._reverse_leaping_first_unaccepted_value

        self._flag += JobStepFlag.reverse

    def _handle_stepping(self):
        # 断言步进状态
        assert JobStepFlag.stepping in self.flag

        try:
            self._step()
        except IndexError:
            if JobStepFlag.reverse not in self.flag:
                raise

        # 相与结果非零即为具有该标志位
        if JobStepFlag.reverse in self.flag:
            self._prepare_stepping()
        else:
            self._prepare_leaping()

    def _handle_leaping(self):
        # 断言跃进状态
        assert JobStepFlag.leaping in self.flag

        try:
            self._leap()
        except IndexError:
            if JobStepFlag.reverse not in self.flag:
                raise
            self._prepare_stepping()
            return

        if JobStepFlag.reverse in self.flag:
            self._prepare_reverse_stepping()
        else:
            self._prepare_reverse_leaping()

    def __safe_handle(self):
        err_info = (None, None, None)
        # noinspection PyBroadException
        try:
            self._emitter.emit("IndexJob.handling", self)
            self._handle()
            self._emitter.emit("IndexJob.handled", self)
            return True
        except ExplicitlySkipHandlingError:
            err_info = sys.exc_info()
            self._emitter.emit("IndexJob.handle_skipped", self, err_info)
            return False
        except (ExplicitlyStopHandlingError, AssertionError) as e:
            raise e
        except Exception:
            err_info = sys.exc_info()
            self._emitter.emit("IndexJob.unexpected_exception", self, err_info)
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
