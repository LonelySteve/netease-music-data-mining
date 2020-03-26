#!/usr/env python3
import math
from typing import Optional, Union

from src.status import IStatus

from .util import repr_injector


@repr_injector
class Span(object):
    def __init__(self, begin: int, end: Union[int, float]):
        # 如果 end 不是无穷大，则对其取整
        if math.fabs(end) != float("inf"):
            end = int(end)

        self.begin = int(begin)
        self.end = end

    def __eq__(self, other):
        if not isinstance(other, Span):
            return False
        return (self.begin, self.end) == (other.begin, other.end)

    def __hash__(self):
        return hash(self.begin) ^ hash(self.end)

    def __len__(self):
        return self.max_val - self.min_val + 1

    def __copy__(self):
        return Span(self.begin, self.end)

    def __str__(self):
        return f"begin={self.begin}, end={self.end}"

    @property
    def min_val(self):
        return min(self.begin, self.end)

    @property
    def max_val(self):
        return max(self.begin, self.end)

    def contain(self, i):
        return self.min_val <= i <= self.max_val


class StepSpan(Span):
    def __init__(self, begin: int, end: Optional[Union[int, float]] = None, step: int = 1):
        if end is None:
            end = math.copysign(float("inf"), step)

        if (begin < end and step <= 0) or (begin > end and step >= 0):
            raise ValueError(
                f"The value of step ({step}) is not ensure to reach the end.")

        self.step: int = int(step)
        super().__init__(begin, end)

    def __eq__(self, other):
        if not isinstance(other, StepSpan):
            return False
        return (self.begin, self.end, self.step) == (other.begin, other.end, other.step)

    def __hash__(self):
        return hash(self.begin) ^ hash(self.end) ^ hash(self.step)

    def __copy__(self):
        return StepSpan(self.begin, self.end, self.step)

    def __str__(self):
        return f"begin={self.begin}, end={self.end}, step={self.step}"


class WorkSpan(StepSpan):

    def __init__(self, begin: int, end: Optional[int] = None, step: int = 1):
        self._worked_span: Optional[Span] = None
        self._current = None

        super().__init__(begin, end, step)

    def __eq__(self, other):
        if not isinstance(other, WorkSpan):
            return False
        return (self.begin, self.end, self.step, self.worked_span, self.current) == (
            other.begin, other.end, other.step, other.worked_span, other.current)

    def __hash__(self):
        return hash(self.begin) ^ hash(self.end) ^ hash(self.step) ^ hash(self.worked_span) ^ hash(self.current)

    def __copy__(self):
        return WorkSpan(self.begin, self.end, self.step)

    @property
    def worked_span(self):
        return self._worked_span

    @property
    def current(self):
        return self._current

    @property
    def processed(self) -> float:
        if self.worked_span is None:
            return 0
        return len(self.worked_span) / len(self)

    def _update_worked_span(self, i):
        if i < self.min_val or self.max_val < i:
            raise IndexError(i)

        if self.worked_span is None:
            self._worked_span = Span(i, i)
            return

        # 数轴方向（增大）->    新 worked span 的 begin 和 end（'-' 表示保持原样）
        # ----------------------------------------------------------------
        # begin i end                   -
        # i begin end                   i end
        # begin end i                   begin i
        # end i begin                   -
        # end begin i                   i end
        # i end begin                   begin i
        if self.worked_span.contain(i):
            return
        if i < self.worked_span.begin or i > self.worked_span.begin:
            self._worked_span = Span(i, self.worked_span.end)
        else:
            self._worked_span = Span(self.worked_span.begin, i)
