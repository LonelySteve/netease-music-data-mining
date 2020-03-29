#!/usr/env python3
import math

import pytest
from src.fetcher import IndexFetcher


def _spilt(jobs_, i_):
    return jobs_[i_].begin, jobs_[i_].end, jobs_[i_].step


@pytest.mark.parametrize("weights", [
    tuple(),
    (1,),
    (1, 1),
    (1, 1, 1),
    (1, 2),
    (2, 1),
])
@pytest.mark.parametrize("begin, end, step", [
    (1, 1, 0),
    (1, 2, 1),
    (2, 1, -1),
    (1, 3, 1),
    (1, 4, 1),
    (3, 1, -1),
    (4, 1, -1),
    (1, 5, 2),
    (1, 6, 2),
    (5, 1, -2),
    (6, 1, -2)
])
def test_job_iter(begin, end, step, weights):
    fetcher = IndexFetcher(begin, end, step, thread_weights=weights)
    jobs = list(fetcher.job_iter())

    if not weights or weights == (1,) or len(fetcher) < len(weights):  # 等效
        assert len(jobs) == 1
        if math.fabs(step) == 1:
            assert _spilt(jobs, 0) == (begin, end, step)
        else:
            if (begin, end, step) in [(1, 5, 2), (1, 6, 2)]:
                assert _spilt(jobs, 0) == (1, 5, 2)
            if (begin, end, step) == (5, 1, -2):
                assert _spilt(jobs, 0) == (5, 1, -2)
            if (begin, end, step) == (6, 1, -2):
                assert _spilt(jobs, 0) == (6, 2, -2)
    else:
        if (begin, end, step) == (1, 2, 1):
            if weights == (1, 1):
                assert len(jobs) == 2
                assert _spilt(jobs, 0) == (1, 1, 1)
                assert _spilt(jobs, 1) == (2, 2, 1)
            elif weights == (1, 2):
                assert len(jobs) == 2
                assert _spilt(jobs, 0) == (1, 1, 1)
                assert _spilt(jobs, 1) == (2, 2, 1)
            elif weights == (2, 1):
                assert len(jobs) == 1
                assert _spilt(jobs, 0) == (1, 2, 1)
        elif (begin, end, step) == (2, 1, -1):
            if weights == (1, 1):
                assert len(jobs) == 2
            elif weights == (1, 2):
                assert len(jobs) == 2
            elif weights == (2, 1):
                assert len(jobs) == 1


def test_job_iter_sample():
    fetcher = IndexFetcher(0, 99, 1, thread_weights=[1, 1])
    jobs = list(fetcher.job_iter())
    assert len(jobs) == 2
    assert _spilt(jobs, 0) == (0, 49, 1)
    assert _spilt(jobs, 1) == (50, 99, 1)

    fetcher = IndexFetcher(0, 99, 1, thread_weights=[1, 2])
    jobs = list(fetcher.job_iter())
    assert len(jobs) == 2
    assert _spilt(jobs, 0) == (0, 33, 1)
    assert _spilt(jobs, 1) == (34, 99, 1)

    fetcher = IndexFetcher(99, 0, -1, thread_weights=[1, 1])
    jobs = list(fetcher.job_iter())
    assert len(jobs) == 2
    assert _spilt(jobs, 0) == (99, 50, -1)
    assert _spilt(jobs, 1) == (49, 0, -1)

    fetcher = IndexFetcher(99, 0, -1, thread_weights=[1, 2])
    jobs = list(fetcher.job_iter())
    assert len(jobs) == 2
    assert _spilt(jobs, 0) == (99, 66, -1)
    assert _spilt(jobs, 1) == (65, 0, -1)
