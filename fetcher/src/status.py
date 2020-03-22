#!/usr/env python3
from abc import ABCMeta, abstractmethod
from datetime import timedelta
from typing import Optional


class Status(metaclass=ABCMeta):

    @property
    @abstractmethod
    def process(self) -> float:
        """当前进度（以浮点数表示，范围 0~1）"""


class ObserverStatus(Status):
    @property
    @abstractmethod
    def average_speed(self) -> float:
        """平均速度（每秒计）"""

    @property
    @abstractmethod
    def assumed_time_remaining(self) -> Optional[timedelta]:
        """推测剩余时间"""
