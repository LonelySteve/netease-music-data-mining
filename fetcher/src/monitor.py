#!/usr/env python3
import statistics
import time
from abc import ABCMeta, abstractmethod
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Thread
from typing import Callable, Dict, Iterable, Optional, Union

from .fetcher import IndexFetcher
from .flag import ThreadFlag
from .job import IndexJob
from .status import IIndexWorkStatus
from .util import repr_injector

_work_thread_factory_type = Optional[Callable[[Callable[[], None]], Thread]]


@repr_injector
class Monitor(IIndexWorkStatus, metaclass=ABCMeta):
    def __init__(self, tick_interval: Optional[Union[int, timedelta]] = None,
                 work_thread_factory: _work_thread_factory_type = None):
        if isinstance(tick_interval, int):
            tick_interval = timedelta(seconds=tick_interval)

        self._flag = ThreadFlag(ThreadFlag.pending)
        self._tick_interval = tick_interval or timedelta(seconds=1)
        self._work_thread_factory = work_thread_factory or (
            lambda work_func: Thread(name=f"{self.__class__.__name__.lower()}-thread", target=work_func)
        )
        self._work_thread: Optional[Thread] = None
        self._start_working_time: Optional[datetime] = None
        self._end_working_time: Optional[datetime] = None
        self._exception: Optional[Exception] = None

    @property
    def work_thread(self):
        return self._work_thread

    @property
    def flag(self):
        return self._flag

    @property
    def age(self):
        if self._start_working_time is None:
            return None
        if self._end_working_time is None:
            return datetime.now() - self._start_working_time

        return self._end_working_time - self._start_working_time

    def start(self):
        if self._work_thread is not None and self._work_thread.is_alive():
            self.stop()

        self._work_thread = self._work_thread_factory(self._work)
        self._work_thread.daemon = True
        self._work_thread.start()

    def stop(self):
        self._flag -= ThreadFlag.running
        self._flag += ThreadFlag.canceling
        while self._work_thread is not None and self._work_thread.is_alive():
            time.sleep(1)
        self._flag -= ThreadFlag.canceling
        self._flag += ThreadFlag.stopping_with_canceled

    def raise_if_has_exception(self):
        if self._exception is not None:
            raise self._exception

    def _work(self):
        self._flag -= ThreadFlag.pending
        self._flag += ThreadFlag.running
        self._start_working_time = datetime.now()
        try:
            while True:
                self._tick()
                time.sleep(self._tick_interval.seconds)
                if ThreadFlag.canceling in self.flag:
                    self._flag -= ThreadFlag.running
                    self._flag -= ThreadFlag.canceling
                    self._flag += ThreadFlag.stopping_with_canceled
                    break
        except Exception as err:
            self._flag -= ThreadFlag.running
            self._flag += ThreadFlag.stopping_with_exception
            self._exception = err
        finally:
            self._end_working_time = datetime.now()

    @abstractmethod
    def _tick(self):
        pass


@dataclass
class JobStatusData(IIndexWorkStatus):
    _tick_interval: timedelta
    job: IndexJob
    start_monitoring_time: datetime
    end_monitoring_time: Optional[datetime] = None
    total_count_of_indexes: int = 1
    total_count_of_valid_indexes: int = 1
    process_deque: deque = deque(maxlen=60)

    @property
    def age(self):
        return datetime.now() - self.start_monitoring_time

    @property
    def flag(self):
        return self.job.flag

    @property
    def processed(self) -> float:
        return self.job.processed

    @property
    def remaining_time(self) -> timedelta:
        """估计的剩余的时间（单位：tick），如果采集数据过少或进度增长速度平均为 0，返回 timedelta.max"""
        if len(self.process_deque) < 2:
            return timedelta.max

        average_progress = statistics.mean(
            self.process_deque[i + 1] - self.process_deque[i] for i in range(len(self.process_deque) - 1)
        )

        if average_progress == 0:
            return timedelta.max

        return self._tick_interval * ((1 - self.job.processed) / average_progress)

    @property
    def average_speed(self) -> float:
        """平均速度（单位：次/秒）"""
        if self.age.seconds == 0:
            return 0
        return self.total_count_of_indexes / self.age.seconds

    @property
    def effective_average_speed(self) -> float:
        """有效平均速度（单位：次/秒）"""
        if self.age.seconds == 0:
            return 0
        return self.total_count_of_valid_indexes / self.age.seconds


class IndexFetcherMonitor(Monitor, IIndexWorkStatus):

    def __init__(self, fetchers: Union[IndexFetcher, Iterable[IndexFetcher]],
                 tick_interval: Union[int, timedelta] = None,
                 work_thread_factory: _work_thread_factory_type = None):
        if isinstance(fetchers, IndexFetcher):
            fetchers = [fetchers]

        self.fetchers = fetchers

        self._monitored_jobs: Dict[IndexJob, JobStatusData] = {}

        super().__init__(tick_interval, work_thread_factory)

    @property
    def monitored_jobs(self):
        return self._monitored_jobs.copy()

    def _tick(self):
        for fetcher in self.fetchers:
            for job in fetcher.jobs:
                if job in self._monitored_jobs:
                    self._monitored_jobs[job].process_deque.append(job.processed)
                    continue

                self._monitored_jobs[job] = JobStatusData(self._tick_interval, job, datetime.now())

                @job.emitter.on("IndexJob.handling")
                def handling_trigger(sender: IndexJob):
                    self._monitored_jobs[sender].total_count_of_indexes += 1

                @job.emitter.on("IndexJob.handled")
                def handled_trigger(sender: IndexJob):
                    self._monitored_jobs[sender].total_count_of_valid_indexes += 1

    @property
    def processed(self) -> Optional[float]:
        """当前进度（以浮点数表示，范围 0~1），如果状态不支持，返回 None"""
        try:
            return statistics.mean(
                job_status_data.processed for job_status_data in self._monitored_jobs.values() if
                job_status_data.job.working
            )
        except statistics.StatisticsError as e:
            return None

    @property
    def average_speed(self) -> Optional[float]:
        """平均速度（每 tick 计），如果状态不支持，返回 None"""
        try:
            return statistics.mean(
                job_status_data.average_speed for job_status_data in self._monitored_jobs.values() if
                job_status_data.job.working
            )
        except statistics.StatisticsError:
            return None

    @property
    def remaining_time(self) -> Optional[timedelta]:
        """推测剩余时间，如果状态不支持，放回 None"""
        return max((job_status_data.remaining_time for job_status_data in self._monitored_jobs.values() if
                    job_status_data.job.working and job_status_data.remaining_time is not None), default=None)

    @property
    def effective_average_speed(self) -> Optional[float]:
        try:
            return statistics.mean(
                job_status_data.effective_average_speed for job_status_data in self._monitored_jobs.values() if
                job_status_data.job.working
            )
        except statistics.StatisticsError:
            return None
