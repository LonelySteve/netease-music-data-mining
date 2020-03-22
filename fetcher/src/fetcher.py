#!/usr/env python3
import math
import statistics
import time
from abc import ABCMeta
from concurrent.futures import (Executor, Future, ThreadPoolExecutor,
                                as_completed, wait)
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import partial
from inspect import Parameter, signature
from itertools import count
from threading import Thread
from typing import Callable, Dict, Iterable, List, Optional

from pyee import AsyncIOEventEmitter

from .job import BaseJob, Handlers, IndexJob
from .span import StepSpan
from .status import ObserverStatus
from .util import jump_step, repr_injector

_executor_factory_type = Optional[Callable[[], Executor]]


class BaseFetcher(metaclass=ABCMeta):
    __counter = 0

    def __init__(self, name=None, emitter=None, thread_weights=None, executor_factory: _executor_factory_type = None):
        self.emitter = emitter or AsyncIOEventEmitter()
        self.thread_weights = self.standardized_thread_weights(thread_weights) or [1]
        self.executor_factory = executor_factory or (lambda: ThreadPoolExecutor(thread_name_prefix=name))

        self._working = False
        self._handlers: Dict[Callable[..., None], Parameter] = {}

    @staticmethod
    def standardized_thread_weights(thread_weights):
        non_positive_weight = next((weight for weight in thread_weights if weight <= 0), None)
        if non_positive_weight:
            raise ValueError(f"Non-positive weights are not accepted: {non_positive_weight!r}")

        total_value = sum(thread_weights)

        return [weight / total_value for weight in thread_weights]


PENDING = "PENDING"
RUNNING = "RUNNING"
STOPPING = "STOPPING"
STARTING = "STARTING"


@repr_injector
class IndexFetcher(BaseFetcher, StepSpan, ObserverStatus):

    def __init__(self, begin: int, end: Optional[int] = None, step: int = 1,
                 jump_step_func: Callable[[], Iterable[int]] = None, jump_step_limit: int = 3,
                 name: Optional[str] = None, emitter=None,
                 thread_weights=None, executor_factory: _executor_factory_type = None):

        self.jump_step_func = jump_step_func or jump_step
        self.jump_step_limit = jump_step_limit
        self.handlers = Handlers()
        self.status = PENDING

        self._contractor_thread = None
        self._observer_thread = None
        self._jobs: List[IndexJob] = []
        self._total_observation_time: Optional[timedelta] = None
        self._total_observation_index_handle_count: int = 0
        self._total_observation_index_valid_handle_count: int = 0
        self._observation_processes = {}

        BaseFetcher.__init__(self, name=name, emitter=emitter, thread_weights=thread_weights,
                             executor_factory=executor_factory)
        StepSpan.__init__(self, begin, end, step)

    @property
    def average_speed(self) -> float:
        if self._total_observation_time is None:
            return 0
        return self._total_observation_index_handle_count / self._total_observation_time.seconds

    @property
    def assumed_time_remaining(self) -> Optional[timedelta]:
        if self._total_observation_time is None:
            return None
        # TODO WIP
        return 

    @property
    def process(self) -> float:
        return statistics.mean(job.process for job in self._jobs)

    def start(self, timeout=None):
        if self._contractor_thread is not None and self._contractor_thread.is_alive():
            raise RuntimeError("The current jobs is not finished and cannot be started again.")

        self.status = STARTING

        self._contractor_thread = Thread(target=self._start, kwargs={"timeout": timeout}, name="contractor_thread")
        self._contractor_thread.daemon = True
        self._contractor_thread.start()

        # 等待上一次的观察者线程退出
        while self._total_observation_time is not None:
            ...
        self.status = RUNNING
        # 开启新的观察者线程
        self._observer_thread = Thread(target=self._observer, name="observer_thread")
        self._observer_thread.daemon = True
        self._observer_thread.start()

    def stop(self):
        for job in self._jobs:
            job.cancel()

    def job_iter(self):
        begin: int = self.begin
        for i, weight in enumerate(self.thread_weights):
            job_len = math.ceil(len(self) * weight)
            end = begin + job_len - 1
            yield self._job_factory(begin, end)
            begin = end + self.step

    def _start(self, timeout=None):
        self._jobs.clear()
        self.emitter.emit("start_all")
        with self.executor_factory() as executor:
            for job in self.job_iter():
                self._jobs.append(job)
                self._job_futures.append(executor.submit(job))
            wait(self._job_futures, timeout=timeout)
        self.emitter.emit("all_completed")

    def _observer(self):
        start_time = datetime.now()
        while self.status == RUNNING:
            self._total_observation_time = datetime.now() - start_time
            time.sleep(1)
        self._total_observation_index_handle_count = 0
        self._total_observation_index_valid_handle_count = 0
        self._total_observation_time = None

    def _watch_total_index_handle_count(self, sender: IndexJob):
        self._total_observation_index_handle_count += 1

    def _watch_total_index_valid_handle_count(self, sender: IndexJob):
        self._total_observation_index_valid_handle_count += 1

    def _job_factory(self, begin, end):
        job = IndexJob(begin, end, self.step, self.jump_step_func,
                       self.jump_step_limit, self.emitter)
        job.handlers = Handlers(self.handlers)
        job.emitter.on("handling", self._watch_total_index_handle_count)
        job.emitter.on("handled", self._watch_total_index_valid_handle_count)
        return job
