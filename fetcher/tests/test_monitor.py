#!/usr/env python3
import time
from datetime import timedelta

from src.fetcher import IndexFetcher
from src.monitor import IndexFetcherMonitor


def test_sample():
    fetcher = IndexFetcher(0, 100, 1)
    monitor = IndexFetcherMonitor(fetcher)

    @fetcher.handlers.add
    def for_test(i, sender):
        time.sleep(0.1)
        print(f"{i}: {monitor}")

    @fetcher.emitter.on("IndexJob.stopped")
    def error_output(sender, err):
        print(f"{sender}: {err!r}")

    monitor.start()
    fetcher.start()
    try:
        fetcher.join()
    finally:
        monitor.stop()
