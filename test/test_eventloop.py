import pytest
import os
import select
import time

import paf.eventloop as eventloop

@pytest.yield_fixture(scope='function')
def event_loop():
    stop_source = eventloop.Source()
    stop_source.set_timeout(time.time() + 0.5)

    event_loop = eventloop.EventLoop()

    event_loop.add(stop_source, lambda: event_loop.stop())

    yield event_loop

    event_loop.remove(stop_source)

NUM_ACTIVE=2
def test_multiple_fds_active_source_removed(event_loop):
    pipes = []
    fd_source = eventloop.Source()
    source_fds = {}

    for i in range(NUM_ACTIVE):
        pipe = os.pipe()
        rfd = pipe[0]
        source_fds[rfd] = select.EPOLLIN
        pipes.append(pipe)

    fd_source.set_fds(source_fds)

    event_loop.add(fd_source, lambda: event_loop.remove(fd_source))

    for rfd, wfd in pipes:
        os.write(wfd, "x".encode('utf-8'))

    event_loop.run()

