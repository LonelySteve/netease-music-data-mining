#!/usr/env python3
import math
import sys
from abc import ABC
from concurrent.futures.process import ProcessPoolExecutor
from contextlib import contextmanager
from queue import Queue, Empty
from typing import Optional, Type, Callable, Iterable

from .event import Event
from .exceptions import ValueChoicesError
from .flag import Flag, TaskFlagGroup

from .task import BaseTask, ParallelTask, IndexTask, IndexTaskFlagGroup

from concurrent.futures import Executor, ThreadPoolExecutor

from .utils import jump_step
from .worker import Worker, WorkerFlagGroup


class UnacceptableTaskError(Exception):
    """不接受的工作异常"""


class Handler(Worker, ABC):
    def __init__(
        self,
        name: Optional[str] = None,
        work_queue: Optional[Queue[BaseTask]] = None,
        status: Optional[WorkerFlagGroup] = None,
    ):
        super().__init__(name, work_queue, status)

    def confirm(self, task: BaseTask) -> bool:
        """
        确定指定 Task 是否可被自己处理

        :param task: 指定 Task
        :return: bool
        """
        raise NotImplementedError

    def add_task(self, task: BaseTask):
        """
        添加 Task 到自己的处理队列中等待被处理

        :param task: 要添加的 job
        :return:
        """
        if not self.confirm(task):
            raise UnacceptableTaskError(task)

        self.work_queue.put(task, block=False)

    def add_tasks(self, tasks: Iterable[BaseTask]):
        """
        批量添加 Task 到自己的处理队列中等待被处理

        :param tasks: 要添加的 Task 类型的可迭代对象
        :return:
        """
        for job in tasks:
            if not self.confirm(job):
                raise UnacceptableTaskError(job)

            self.work_queue.put(job)

    @contextmanager
    def work_session(self):
        self.status.unset(WorkerFlagGroup.pending)
        self.status.set(WorkerFlagGroup.running)
        try:
            yield
        finally:
            # 先取消 running 标志，再置 stopping 标志后
            self.status.unset(WorkerFlagGroup.running)
            self.status.set(WorkerFlagGroup.stopping)


class ParallelTaskHandler(Handler):
    event_parallel_task_begin: Event
    event_parallel_task_end: Event
    event_handle_one_task_begin: Event
    event_handle_one_task_end: Event
    event_handle_one_task_raise_error: Event

    def __init__(
        self,
        name: Optional[str] = None,
        work_queue: Optional[Queue[BaseTask]] = None,
        status: Optional[WorkerFlagGroup] = None,
        max_parallel_count: Optional[int] = None,
        executor_cls: Type[
            ThreadPoolExecutor, ProcessPoolExecutor
        ] = ThreadPoolExecutor,
        initiallizer: Optional[Callable[[], None]] = None,
    ):
        super().__init__(name, work_queue, status)

        supported_executor_cls = (ThreadPoolExecutor, ProcessPoolExecutor)

        if executor_cls not in (ThreadPoolExecutor, ProcessPoolExecutor):
            raise ValueChoicesError(
                supported_executor_cls, executor_cls, "executor_cls"
            )

        self._executor_factory: Callable[[], Executor] = executor_cls(
            max_workers=max_parallel_count, initiallizer=initiallizer
        )

    def confirm(self, task: BaseTask) -> bool:
        return isinstance(task, ParallelTask)

    def tick(self):
        task = self.task_queue.get()
        self.event_parallel_task_begin.emit(self, task)
        with self._executor_factory() as executor:
            futures = (
                executor.submit(self.wrap_handle_one, task)
                for task in task.chunk_iter()
            )
        self.event_parallel_task_end.emit(self, futures)

    def wrap_handle_one(self, task):
        self.event_handle_one_task_begin.emit(self, task)

        # noinspection PyBroadException
        try:
            self.handle_one(task)
        except Exception:
            self.event_handle_one_task_raise_error.emit(self, *sys.exc_info())

        self.event_handle_one_task_end.emit(self, task)

    def handle_one(self, task):
        """
        针对单个任务的处理实现

        :param task: 要处理的任务
        :return:
        """
        raise NotImplementedError


class IndexTaskHandler(ParallelTaskHandler):
    def __init__(
        self,
        max_parallel_count: Optional[int] = None,
        executor_cls: Type[
            ThreadPoolExecutor, ProcessPoolExecutor
        ] = ThreadPoolExecutor,
        jump_step_func: Callable[[], Iterable[int]] = None,
    ):
        super().__init__(
            max_parallel_count=max_parallel_count, executor_cls=executor_cls,
        )

        self.jump_step_func = jump_step_func or jump_step

    def confirm(self, task: BaseTask) -> bool:
        return isinstance(task, IndexTask)

    def handle_one(self, task: IndexTask):
        """
        针对单个任务的处理实现

        :param task: 要处理的任务
        :return:
        """
        with self.work_session():
            task.status.unset(IndexTaskFlagGroup.pending)
            task.current = task.begin
            # noinspection PyBroadException
            try:
                task.status.set(IndexTaskFlagGroup.stepping)
                # 循环处理，下面是状态简化图：
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
                    if task.status.all("stepping|reserve"):
                        self._handle_stepping(task, reserve=True)
                    elif task.status.all("leaping|reserve"):
                        self._handle_leaping(task, reserve=True)
                    elif task.status.has("stepping"):
                        self._handle_stepping(task, reserve=False)
                    elif task.status.has("leaping"):
                        self._handle_leaping(task, reserve=False)
                    else:
                        raise RuntimeError(f"invalid IndexTask status: {task.status}")
            except IndexError:
                pass
            finally:
                task.current = None
                task.status.set(TaskFlagGroup.stopping)

    def _jumper(self, begin, sign):
        i = begin
        for d in self.jump_step_func():
            i += int(math.copysign(d, sign))
            yield i

    def _step(self, task: IndexTask):
        if self.job_span.step == 0:
            self._set_current(self.job_span.begin)
            self.__safe_handle()
            return

        for i in count(self.current, self.job_span.step):
            self._try_cancel()
            self._set_current(i)
            if not self.__safe_handle():
                break

    def _leap(self, task: IndexTask):
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
        self.job_span.step = -self.job_span.step
        self.job_span.begin = self._current
        self.job_span.end = self._reverse_leaping_first_unaccepted_value

        self._flag += JobStepFlag.reverse

    def _handle_stepping(self, task: IndexTask, reserve=False):
        try:
            self._step(task)
        except IndexError:
            if not reserve:
                raise

        if reserve:
            self._prepare_stepping()
        else:
            self._prepare_leaping()

    def _handle_leaping(self, task: IndexTask, reserve=False):
        try:
            self._leap(task)
        except IndexError:
            if reserve:
                self._prepare_stepping()
                return

            raise

        if reserve:
            self._prepare_reverse_stepping()
        else:
            self._prepare_reverse_leaping()

    def __safe_handle(self):
        err_info = (None, None, None)
        # noinspection PyBroadException
        try:
            self._emitter.emit("IndexJob.handling", self)
            self.handle_one()
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
