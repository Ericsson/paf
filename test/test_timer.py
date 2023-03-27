# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2023 Ericsson AB

import paf.timer
import time
import random


def test_remove_timers():
    manager = paf.timer.TimerManager()

    now = time.time()

    num_timers = 100
    timeouts = [now + random.random() for _ in range(num_timers)]
    timers = [manager.add(lambda x: None, timeout) for timeout in timeouts]

    random.shuffle(timers)
    timeouts.sort()

    for timer in timers:
        manager.remove(timer)

        timeouts.remove(timer.expiration_time)

        if len(timeouts) > 0:
            assert manager.next_timeout() == timeouts[0]
        else:
            assert manager.next_timeout() is None


def test_expire_timers():
    manager = paf.timer.TimerManager()

    now = time.time()

    num_timers = 1000
    max_timeout = 1.0
    fired = []

    for _ in range(num_timers):
        timeout = now + random.random() * max_timeout

        def handler(timeout=timeout):
            assert timeout <= time.time()
            fired.append(timeout)

        manager.add(handler, timeout)

    deadline = time.time() + max_timeout * 2

    while time.time() < deadline:
        then = time.time()

        manager.process()

        next_timeout = manager.next_timeout()

        if next_timeout is not None:
            assert next_timeout > then

    assert len(fired) == num_timers

    assert fired == list(sorted(fired))

    assert manager.next_timeout() is None
