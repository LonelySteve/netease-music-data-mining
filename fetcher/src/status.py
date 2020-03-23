#!/usr/env python3
from abc import ABCMeta, abstractmethod
from datetime import timedelta
from enum import IntFlag, unique
from typing import Optional


class IStatus(metaclass=ABCMeta):

    @property
    @abstractmethod
    def process(self) -> float:
        """当前进度（以浮点数表示，范围 0~1）"""


class IWatcherStatus(IStatus):
    @property
    @abstractmethod
    def average_speed(self) -> float:
        """平均速度（每秒计）"""

    @property
    @abstractmethod
    def assumed_time_remaining(self) -> Optional[timedelta]:
        """推测剩余时间"""


@unique
class StatusFlag(IntFlag):
    # 状态标志位
    # 目前一共有 7 位，每一位的解释如下：
    #
    #  最高位        最低位
    #    v           v
    #    0 0 0 0 0 0 0
    #
    # - 最低位：标识基础任务状态，0 表示就绪，1 表示结束
    # - 第二位：表示异常结束状态，0 表示 False，1 表示 True
    # - 第三位：表示取消结束状态，0 表示 False，1 表示 True
    # - 第四位：表示运行状态，0 表示 False，1 表示 True
    # - 第五位：表示取消状态，0 表示 False，1 表示 True
    # - 第六位：表示步进/跃进状态，0 表示步进，1 表示跃进
    # - 最高位：表示反转状态，0 表示 False，1 表示 True

    pending = ~0b0000001
    stopping = 0b0000001
    stopping_with_exception = 0b0000010
    stopping_with_canceled = 0b0000100
    running = 0b0001000
    canceling = 0b0010000
    stepping = ~0b0100000
    leaping = 0b0100000
    reverse = 0b1000000
