#!/usr/env python3
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Optional


class IStatus(metaclass=ABCMeta):
    @property
    @abstractmethod
    def flag(self):
        ...


class IWorkStatus(IStatus):

    @property
    @abstractmethod
    def age(self):
        ...


class IProgressiveStatus(IWorkStatus):

    @property
    @abstractmethod
    def processed(self):
        ...

    @property
    @abstractmethod
    def remaining_time(self):
        ...

    @property
    @abstractmethod
    def average_speed(self):
        ...


class IIndexWorkStatus(IProgressiveStatus):
    @property
    @abstractmethod
    def effective_average_speed(self):
        ...
