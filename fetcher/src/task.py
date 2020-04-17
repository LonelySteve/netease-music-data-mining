#!/usr/env python3
import math

from typing import Optional

from .flag import Flag, TaskFlagGroup
from .utils import sign


class BaseTask(object):
    def __init__(self, status: Optional[TaskFlagGroup] = None):
        self._status = status or TaskFlagGroup(TaskFlagGroup.pending)

    @property
    def status(self):
        return self._status.copy()

    @property
    def working(self):
        return self._status.has(TaskFlagGroup.running)


class ParallelTask(BaseTask):
    def __init__(self, chuck_count=1, status: Optional[TaskFlagGroup] = None):
        super().__init__(status)

        self._chunk_count = chuck_count

    @property
    def chunk_count(self):
        return self._chunk_count

    def chunk_iter(self):
        if self._chunk_count <= 1:
            yield self


class IndexTaskFlagGroup(TaskFlagGroup):
    leaping = Flag(parents="running")
    stepping = Flag(parents="running")
    reverse = Flag(parents="running|leaping|stepping")

    def _get_mutex_groups(self):
        return super()._get_mutex_groups() + [[self.leaping, self.stepping]]


class IndexTask(ParallelTask):
    def __init__(
        self,
        api: str,
        begin: int,
        end: Optional[int] = None,
        step: int = 1,
        chuck_count=1,
    ):

        if end is None:
            end = math.copysign(float("inf"), step)

        if (begin < end and step <= 0) or (begin > end and step >= 0):
            raise ValueError(
                f"the value of step ({step}) is not ensure to reach the end"
            )

        self._begin = int(begin)
        self._end = end
        self._step = int(step)

        self._worked_counter = 0
        self._current: Optional[int] = None

        self._api = api

        # 自己的长度有可能为 inf，这种情况下，chunk_count 将始终为 1
        if len(self) == float("inf"):
            chuck_count = 1

        super().__init__(chuck_count)

    def __len__(self):
        # https://stackoverflow.com/questions/31839032/python-how-to-calculate-the-length-of-a-range-without-creating-the-range
        return (self._end - self._begin - 1) // self._step + 1

    @property
    def begin(self):
        return self._begin

    @property
    def end(self):
        return self._end

    @property
    def step(self):
        return self._step

    @property
    def api(self):
        return self._api

    @property
    def worked_counter(self):
        return self._worked_counter

    @property
    def current(self):
        return self._current

    @current.setter
    def current(self, value):
        if value is None:
            self._current = value
            return

        s = sign(self.step)

        if (
            s * value < s * self._begin
            or s * value >= s * self._end
            or (value != self._begin and value != self._end)
        ):
            raise IndexError(value)

        self._current = value
        self._worked_counter += 1

    def chunk_iter(self):
        # 先从基类的同名生成器中测试迭代，因为基类的实现会处理 chuck_count <= 1 的情况，所以这之后，只需要处理 chuck_count > 1 的情况
        yield from super().chunk_iter()

        if self.chunk_count <= 1:
            return

        self_length = len(self)
        extra_chunk_size = self_length % self.chunk_count
        full_chunk_count = (
            self.chunk_count if extra_chunk_size == 0 else self.chunk_count - 1
        )
        full_chunk_size = self_length // full_chunk_count

        for i in range(full_chunk_count):
            yield IndexTask(
                self._api,
                self._begin + i * full_chunk_size * self._step,
                self._begin + (i + 1) * full_chunk_size * self._step,
                self._step,
            )

        if extra_chunk_size:
            yield IndexTask(
                self._api,
                self._begin + full_chunk_count * full_chunk_size * self._step,
                self._end,
                self._step,
            )


class TestTask(BaseTask):
    def __init__(self, data):
        super().__init__()
        self.data = data
