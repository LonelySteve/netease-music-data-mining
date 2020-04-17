#!/usr/env python3
import sys
from queue import Queue
from threading import Thread
from typing import Optional


from .flag import Flag, TaskFlagGroup
from .task import BaseTask
from .event import EventSystem, Event


class WorkerFlagGroup(TaskFlagGroup):
    suspending = Flag()

    def _get_mutex_groups(self):
        return [
            [
                self.pending,
                self.running,
                self.stopping,
                self.canceling,
                self.suspending,
            ],
            [self.stopping_with_exception, self.stopping_with_canceled],
        ]


class Worker(EventSystem):
    """
    工作者
    ------
    包装了 Thread 实例，需要注意的是子类应当实现 tick 方法而不是 run 方法，工作将以 tick 为一个单位，
    一个 tick 过去后，工作者将检查其他线程是否要求自己等待或取消当前工作

    """

    event_tick_begin: Event
    event_tick_end: Event
    event_tick_raise_error: Event
    event_cancel: Event

    def __init__(
        self,
        name: Optional[str] = None,
        work_queue: Optional[Queue[BaseTask]] = None,
        status: Optional[WorkerFlagGroup] = None,
    ):
        super().__init__()

        name = name or self.__class__.__name__ + "Thread"
        work_queue = work_queue or Queue[BaseTask]()

        self._work_thread = Thread(name=name, target=self.run)
        self._work_queue: Queue[BaseTask] = work_queue
        self._status = status or WorkerFlagGroup(WorkerFlagGroup.pending)

    @property
    def status(self):
        return self._status.copy()

    @property
    def work_queue(self):
        return self._work_queue

    def start(self):
        if self.status.has(WorkerFlagGroup.stopping):
            raise RuntimeError("The current worker has stopped working and cannot start working again")

        self._work_thread.start()

    def suspend(self):
        self._status.set(WorkerFlagGroup.suspending)

    def resume(self):
        self._status.unset(WorkerFlagGroup.suspending)

    def cancel(self):
        self._status.set(WorkerFlagGroup.canceling)

    def wait(self, timeout=None):
        if self._work_thread is not None:
            self._work_thread.join(timeout)

    def run(self):
        while True:
            try:
                self.event_tick_begin.emit(self)
                self.tick()
                self.event_tick_end.emit(self)
            except Exception:
                self.event_tick_raise_error.emit(self, *sys.exc_info())
                self._status.set(WorkerFlagGroup.stopping_with_exception)
                raise
            self._status.wait_has(WorkerFlagGroup.suspending)
            if self._status.has(WorkerFlagGroup.canceling):
                self._status.unset(WorkerFlagGroup.canceling)
                self._status.set(WorkerFlagGroup.stopping_with_canceled)
                self.event_cancel.emit(self)
                return

    def tick(self):
        raise NotImplementedError

    def is_alive(self):
        if self._work_thread is not None:
            return self._work_thread.is_alive()

    @property
    def ident(self):
        if self._work_thread is not None:
            return self._work_thread.ident

    @property
    def name(self):
        if self._work_thread is not None:
            return self._work_thread.name

    @name.setter
    def name(self, value):
        if self._work_thread is not None:
            self._work_thread.name = value
