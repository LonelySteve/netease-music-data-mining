#!/usr/env python3
import math
from abc import ABC
from concurrent.futures import Executor, Future, ThreadPoolExecutor, as_completed
from inspect import Parameter
from itertools import islice
from queue import Queue
from typing import Callable, Dict, Iterable, List, Optional

from .dispatcher import JobDispatcher


class JobQueueBusyError(Exception):
    """
    作业队列忙时错误
    --------------
    由于作业队列到达繁忙标准，故抛出此异常通知调用方
    """


class Fetcher(object):
    def __init__(self, job_queue_max_size=0, job_queue_busy_percentage=0.8):
        self._job_queue_busy_percentage = job_queue_busy_percentage
        self._job_queue_max_size = job_queue_max_size
        self._job_queue = Queue(self._job_queue_max_size)
        self._job_dispatcher = JobDispatcher(self._job_queue)
        self._status = False

    @property
    def status(self):
        return self._status

    @property
    def job_dispatcher(self):
        return self._job_dispatcher

    def start(self):
        self._job_dispatcher.start()

    def stop(self):
        self._job_dispatcher.stop()

    def add_job(self, job):
        if (
            self._job_queue.qsize() / self._job_queue_max_size
            >= self._job_queue_busy_percentage
        ):
            raise JobQueueBusyError

        self._job_queue.put(job, block=False)

    def join(self):
        pass

    def hold(self, until: str):
        pass
