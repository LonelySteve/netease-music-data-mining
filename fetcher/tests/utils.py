#!/usr/env python3

from contextlib import contextmanager
from datetime import datetime
from typing import Callable


@contextmanager
def assert_time_cost(predicate: Callable[[float], bool]):
    start_time = datetime.now()
    try:
        yield
    finally:
        assert predicate((datetime.now() - start_time).total_seconds())
