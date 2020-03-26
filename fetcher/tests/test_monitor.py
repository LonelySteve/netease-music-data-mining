#!/usr/env python3
from datetime import timedelta

from src.fetcher import IndexFetcher
from src.monitor import IndexFetcherMonitor
from src.flag import JobStepFlag
import time


def test_sample():
    fetcher = IndexFetcher(0, 99, 1)
    monitor = IndexFetcherMonitor(fetcher, timedelta(seconds=0.001))
    print()

    @fetcher.handlers.add
    def for_test(i, sender):
        assert JobStepFlag.running in monitor.flag

        if i == 0:
            assert monitor.processed == 0.01
            assert monitor.effective_average_speed == 0
            assert monitor.average_speed == 0
            assert monitor.remaining_time.seconds == 5
        elif i == 49:
            assert monitor.processed == 0.5
            assert monitor.effective_average_speed > 0
            assert monitor.average_speed > 0
            assert monitor.remaining_time.seconds > 0

        time.sleep(0.01)

    @fetcher.emitter.on("IndexJob.stopped")
    def error_output(sender, err):
        print(f"{sender}: {err!r}")

    monitor.start()
    fetcher.start()
    try:
        fetcher.join()
    finally:
        monitor.stop()
