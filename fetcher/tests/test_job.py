#!/usr/env python3
import math

import pytest

from src.job import IndexJob


def test_instantiate():
    IndexJob(1, 100, 1)
    IndexJob(100, 1, -1)
    with pytest.raises(ValueError):
        IndexJob(1, 100, -1)


@pytest.mark.parametrize("begin, end, step", [
    (1, 100, 1), (100, 1, -1),
    (1, 100, 2), (100, 1, -2),
    (-1, -100, -1), (-100, -1, 1),
    (-1, -100, -2), (-100, -1, 2),
])
def test_general(begin, end, step):
    job = IndexJob(begin, end, step)
    # job 的范围是 [begin, end]，故 range 对应的范围是 [begin, int(end + math.copysign(1, step)))
    with job.list() as result:
        assert result == list(range(begin, int(end + math.copysign(1, step)), step))


@pytest.mark.parametrize("begin, end, step, mock_invalid_values, emitted_values", [
    (1, 10, 1, [1, 2], [3, 4, 5, 6, 7, 8, 9, 10]),
    (1, 10, 1, [2, 3], [1, 4, 5, 6, 7, 8, 9, 10]),
    (1, 10, 1, [1, 2, 3], [5, 4, 6, 7, 8, 9, 10]),
    (1, 10, 1, [2, 3, 4], [1, 6, 5, 7, 8, 9, 10]),
    (1, 10, 1, [1, 2, 3, 4, 5], [7, 6, 8, 9, 10]),
    (10, 1, -1, [1, 2], [10, 9, 8, 7, 6, 5, 4, 3]),
    (10, 1, -1, [8, 9, 10], [6, 7, 5, 4, 3, 2, 1]),
    (10, 1, -1, [6, 8, 9, 10], [4, 5, 3, 2, 1]),
    (10, 1, -1, [4, 6, 8, 9, 10], []),
    (1, 20, 2, [3, 4, 5, 7, 9, 13], [1, 17, 16, 14, 12, 10, 8, 6, 19]),
    (20, 1, -2, [18, 17, 16, 14, 12], [20, 8, 9, 11, 13, 15, 6, 4, 2])
])
def test_jump_back(begin, end, step, mock_invalid_values, emitted_values):
    job = IndexJob(begin, end, step)

    def assertion_conditions(i):
        assert i not in mock_invalid_values

    with job.list(assertion_conditions) as result:
        assert result == emitted_values


def test_repr():
    job = IndexJob(1, 100, 1)
    repr(job)
