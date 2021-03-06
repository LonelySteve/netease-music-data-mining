#!/usr/env python3
import math
from abc import ABC
from concurrent.futures import (Executor, Future, ThreadPoolExecutor,
                                as_completed)
from inspect import Parameter
from itertools import islice
from typing import Callable, Dict, Iterable, List, Optional

from pyee import AsyncIOEventEmitter

from .flag import ThreadFlag
from .job import Handlers, IndexJob
from .span import StepSpan
from .status import IStatus
from .util import jump_step, repr_injector

_executor_factory_type = Optional[Callable[[], Executor]]


class BaseFetcher(IStatus, ABC):
    __counter = 0

    def __init__(self, name=None, emitter=None, thread_weights=None, executor_factory: _executor_factory_type = None):
        self.name = name or self.__class__.__name__
        self.thread_weights = thread_weights or [1]

        self._emitter = emitter or AsyncIOEventEmitter()
        self._flag = ThreadFlag(ThreadFlag.pending)
        self._handlers: Dict[Callable[..., None], Parameter] = {}
        self._executor_factory = executor_factory or (
            lambda: ThreadPoolExecutor(thread_name_prefix=self.name)
        )

    def __str__(self):
        return f"[{self.__class__.__name__}]" \
               f" thread_weights={self.thread_weights!r}" \
               f" working={self.working!r}" \
               f" flag={self._flag!s}"

    @property
    def working(self):
        return ThreadFlag.running in self._flag

    @property
    def flag(self):
        return self._flag

    @property
    def thread_weights(self):
        return self._thread_weights

    @thread_weights.setter
    def thread_weights(self, value):
        self._thread_weights = self.standardized_thread_weights(value)

    @staticmethod
    def standardized_thread_weights(thread_weights):
        non_positive_weight = next((weight for weight in thread_weights if weight >= 0), None)
        if non_positive_weight is None:
            raise ValueError(f"Non-positive weights are not accepted: {non_positive_weight!r}")

        total_value = sum(thread_weights)

        return [weight / total_value for weight in thread_weights]


@repr_injector
class IndexFetcher(BaseFetcher, StepSpan):

    def __init__(self, begin: int, end: Optional[int] = None, step: int = 1,
                 jump_step_func: Callable[[], Iterable[int]] = None, name: Optional[str] = None, emitter=None,
                 thread_weights=None, executor_factory: _executor_factory_type = None):

        self.jump_step_func = jump_step_func or jump_step
        self.handlers = Handlers()

        self._jobs: List[IndexJob] = []
        self._job_futures: Dict[IndexJob, Future] = {}
        self._executor: Optional[Executor] = None

        BaseFetcher.__init__(self, name=name, emitter=emitter, thread_weights=thread_weights,
                             executor_factory=executor_factory)
        StepSpan.__init__(self, begin, end, step)
        # 如果自己的区间长度还没有线程权重长，那么将退化为使用一个线程，即线程权重为 [1]
        if len(self) < len(self.thread_weights):
            self.thread_weights = [1]

    def __str__(self):
        return BaseFetcher.__str__(self) + f" job_count={len(self._jobs)} " + StepSpan.__str__(self)

    @property
    def jobs(self):
        return self._jobs.copy()

    @property
    def emitter(self):
        return self._emitter

    def start(self):
        if ThreadFlag.stopping in self._flag:
            raise RuntimeError(f"Cannot stop a Fetcher that has already stopped.")
        self._jobs.clear()
        self._job_futures.clear()
        self._executor = self._executor_factory()
        for job in self.job_iter():
            self._jobs.append(job)
            self._job_futures[job] = self._executor.submit(job)
        self._flag -= ThreadFlag.pending
        self._flag += ThreadFlag.running

    def join(self, timeout=None):
        for future in as_completed(self._job_futures.values(), timeout=timeout):
            exc = future.exception(timeout=timeout)
            if exc is not None:
                raise exc  # 理论上来讲这里只会抛出 AssertionError

    def stop(self, timeout=None):
        if ThreadFlag.pending in self._flag:
            return
        for job in self._jobs:
            job.cancel()
        self.join(timeout)
        self._executor.shutdown()
        self._flag -= ThreadFlag.running
        self._flag += ThreadFlag.stopping

    def job_iter(self):
        if self.step == 0:
            all_indexes_to_work = [self.begin]
        else:
            all_indexes_to_work = range(self.begin, int(self.end + math.copysign(1, self.step)), self.step)

        def i_get(i_, default=None):
            return next(islice(all_indexes_to_work, i_, i_ + 1), default)

        chuck_size_counter = 0
        for i, weight in enumerate(self.thread_weights):
            # len(all_indexes_to_work) > 0  0 < weight <= 1
            # ->  0 < len(all_indexes_to_work) * weight <= len(all_indexes_to_work)
            # ->  1 <= math.ceil(len(all_indexes_to_work) * weight) <= len(all_indexes_to_work)
            # chuck_size = math.ceil(len(all_indexes_to_work) * weight) - 1
            # ->  0 <= chuck_size <= len(all_indexes_to_work) - 1
            chuck_size = math.ceil(len(all_indexes_to_work) * weight) - 1
            job_begin = i_get(chuck_size_counter)
            chuck_size_counter += chuck_size
            job_end = i_get(chuck_size_counter)
            chuck_size_counter += 1
            if i == len(self.thread_weights) - 1 and job_end is None:
                job_end = i_get(len(all_indexes_to_work) - 1)
            if job_begin is not None:
                yield self._job_factory(job_begin, job_end)

    def _job_factory(self, begin, end):
        job = IndexJob(begin, end, self.step, self.jump_step_func, self.emitter)
        # 继承自身的处理器
        job.handlers = Handlers(self.handlers)
        return job
