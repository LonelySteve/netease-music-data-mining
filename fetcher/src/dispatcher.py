#!/usr/env python3
from queue import Queue
from typing import List

from .handler import Handler


class JobDispatcher(object):
    def __init__(self, job_queue: Queue):
        self._job_queue = job_queue
        self._handlers: List[Handler] = []

    @property
    def handlers(self):
        return self._handlers.copy()

    def append_handler(self, *handler: Handler):
        pass

    def remove_handler(self, *handler: Handler):
        pass

    def start(self):
        pass

    def stop(self):
        pass
