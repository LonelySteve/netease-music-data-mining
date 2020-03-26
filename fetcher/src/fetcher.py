#!/usr/env python3
import math
import statistics
import time
from abc import ABCMeta, ABC
from collections import deque
from concurrent.futures import (Executor, Future, ThreadPoolExecutor,
                                as_completed, wait)
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import partial
from inspect import Parameter, signature
from itertools import count
from queue import Queue
from threading import Thread
from typing import Callable, Dict, Iterable, List, Optional, Union

from pyee import AsyncIOEventEmitter

from .job import Handlers, IndexJob
from .span import StepSpan
from .status import IStatus
from .util import jump_step, repr_injector
from itertools import islice
from .flag import ThreadFlag

_executor_factory_type = Optional[Callable[[], Executor]]


class BaseFetcher(IStatus, ABC):
    __counter = 0

    def __init__(self, name=None, emitter=None, thread_weights=None, executor_factory: _executor_factory_type = None):
        self.emitter = emitter or AsyncIOEventEmitter()
        self.thread_weights = thread_weights or [1]
        self.executor_factory = executor_factory or (lambda: ThreadPoolExecutor(thread_name_prefix=name))

        self._working = False
        self._flag = ThreadFlag(ThreadFlag.pending)
        self._handlers: Dict[Callable[..., None], Parameter] = {}

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
                 jump_step_func: Callable[[], Iterable[int]] = None, jump_step_limit: int = 3,
                 name: Optional[str] = None, emitter=None,
                 thread_weights=None, executor_factory: _executor_factory_type = None):

        self.jump_step_func = jump_step_func or jump_step
        self.jump_step_limit = jump_step_limit
        self.handlers = Handlers()

        self._jobs: List[IndexJob] = []
        self._job_futures: Dict[IndexJob, Future] = {}

        BaseFetcher.__init__(self, name=name, emitter=emitter, thread_weights=thread_weights,
                             executor_factory=executor_factory)
        StepSpan.__init__(self, begin, end, step)
        # 如果自己的区间长度还没有线程权重长，那么将退化为使用一个线程，即线程权重为 [1]
        if len(self) < len(self.thread_weights):
            self.thread_weights = [1]

    @property
    def jobs(self):
        return self._jobs.copy()

    def start(self):
        self._jobs.clear()
        self._job_futures.clear()
        with self.executor_factory() as executor:
            for job in self.job_iter():
                self._jobs.append(job)
                self._job_futures[job] = executor.submit(job)
            self._flag -= ThreadFlag.pending
            self._flag += ThreadFlag.running

    def join(self, timeout=None):
        for future in as_completed(self._job_futures.values(), timeout=timeout):
            exc = future.exception(timeout=timeout)
            if exc is not None:
                raise exc

    def stop(self, timeout=None):
        for job in self._jobs:
            job.cancel()
        self.join(timeout)
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
