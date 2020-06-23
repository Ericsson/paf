import paf.xcm as xcm
import select
import time
import collections
import signal
import fcntl
import os
import errno

def translate(xcm_event):
    mask = 0
    if xcm_event&xcm.FD_READABLE:
        mask |= select.EPOLLIN
    if xcm_event&xcm.FD_WRITABLE:
        mask |= select.EPOLLOUT
    return mask

def set_nonblocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

class Source:
    def __init__(self):
        self.fds = None
        self.timeout = None
        self.active = False
        self.listener = None
    def set_fds(self, fds):
        if self.fds != fds:
            self.fds = fds
            self.dispatch_changed_fds()
    def clear_fds(self):
        if self.fds != None:
            self.fds = None
            self.dispatch_changed_fds()
    def set_timeout(self, timeout):
        if self.timeout != timeout:
            self.timeout = timeout
            self.dispatch_changed_timeout()
    def clear_timeout(self):
        if self.timeout != None:
            self.timeout = None
            self.dispatch_changed_timeout()
    def set_active(self):
        if not self.active:
            self.active = True
            self.dispatch_changed_active()
    def clear_active(self):
        if self.active:
            self.active = False
            self.dispatch_changed_active()
    def set_listener(self, listener):
        self.listener = listener
    def clear_listener(self):
        self.listener = None
    def dispatch_changed_timeout(self):
        if self.listener != None:
            self.listener.changed_timeout(self)
    def dispatch_changed_fds(self):
        if self.listener != None:
            self.listener.changed_fds(self)
    def dispatch_changed_active(self):
        if self.listener != None:
            self.listener.changed_active(self)

class XcmSource (Source):
    def __init__(self, xcm_sock):
        Source.__init__(self)
        self.xcm_sock = xcm_sock
    def update(self, condition):
        xcm_fds, xcm_events = self.xcm_sock.want(condition)
        self.clear_active()
        if len(xcm_fds) == 0:
            if condition != 0:
                self.set_active()
            else:
                self.clear_fds()
        else:
            fds = {}
            for xcm_fd, xcm_event in zip(xcm_fds, xcm_events):
                fds[xcm_fd] = translate(xcm_event)
            self.set_fds(fds)

EPOLL_MAX_TIMEOUT = (((1<<31)-1)/1000)

class EventLoop:
    def __init__(self):
        self.source_handler = {}
        self.source_fds = {}
        self.source_timeout = {}
        self.source_active = set()
        self.fd_source = {}
        self.stop = False
        self.epoll = select.epoll()
        self.init_signal_wakeup_fd()
    def init_signal_wakeup_fd(self):
        for signo in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            signal.signal(signo, lambda signo, frame: None)
        self.s_rfd, self.s_wfd = os.pipe()
        set_nonblocking(self.s_rfd)
        set_nonblocking(self.s_wfd)
        signal.set_wakeup_fd(self.s_wfd)
        self.epoll.register(self.s_rfd, select.EPOLLIN)
    def add(self, source, handler):
        self.source_handler[source] = handler
        self._register_timeout(source)
        self._register_fds(source)
        self._register_active(source)
        source.set_listener(self)
    def remove(self, source):
        source.clear_listener()
        self._unregister_fds(source)
        self._unregister_timeout(source)
        self._unregister_active(source)
        del self.source_handler[source]
    def changed_fds(self, source):
        self._unregister_fds(source)
        self._register_fds(source)
    def _register_fds(self, source):
        if source.fds != None:
            fds = source.fds.copy()
            self.source_fds[source] = fds
            for fd, mask in fds.items():
                self.epoll.register(fd, mask)
                self.fd_source[fd] = source
    def _unregister_fds(self, source):
        fds = self.source_fds.get(source)
        if fds != None:
            del self.source_fds[source]
            for fd, mask in fds.items():
                self.epoll.unregister(fd)
                del self.fd_source[fd]
    def changed_timeout(self, source):
        self._unregister_timeout(source)
        self._register_timeout(source)
    def _register_timeout(self, source):
        if source.timeout != None:
            self.source_timeout[source] = source.timeout
    def _unregister_timeout(self, source):
        if source in self.source_timeout:
            del self.source_timeout[source]
    def changed_active(self, source):
        self._unregister_active(source)
        self._register_active(source)
    def _register_active(self, source):
        if source.active:
            self.source_active.add(source)
    def _unregister_active(self, source):
        if source in self.source_active:
            self.source_active.remove(source)
    def next_relative_timeout(self):
        timeouts = sorted(self.source_timeout.values())
        if len(timeouts) == 0:
            return -1
        else:
            left = timeouts[0] - time.time()
            if left > EPOLL_MAX_TIMEOUT:
                return EPOLL_MAX_TIMEOUT
            if left > 0:
                return left
            return 0
    def fire_timeouts(self):
        if len(self.source_timeout) > 0:
            now = time.time()
            active_handlers = []
            for source, timeout in self.source_timeout.items():
                if now >= timeout:
                    handler = self.source_handler[source]
                    active_handlers.append(handler)
            for handler in active_handlers:
                handler()
    def fire_actives(self):
        while len(self.source_active) > 0:
            source = next(iter(self.source_active))
            handler = self.source_handler[source]
            handler()
    def check_signal(self):
        try:
            os.read(self.s_rfd, 1)
            self.stop = True
        except OSerror:
            pass
    def handle_fds(self, fds):
        for fd, event in fds:
            if fd == self.s_rfd:
                self.check_signal()
            else:
                source = self.fd_source[fd]
                handler = self.source_handler[source]
                handler()
    def run(self):
        self.stop = False
        while not self.stop:
            try:
                self.fire_actives()

                timeout = self.next_relative_timeout()

                if timeout == 0:
                    self.fire_timeouts()
                else:
                    fds = self.epoll.poll(timeout=timeout)
                    if len(fds) > 0:
                        self.handle_fds(fds)
                    else:
                        self.fire_timeouts()

            except IOError as e:
                if e.errno == errno.EINTR:
                    # Python 2 generates an exception on signals (since EINTR)
                    self.stop = True
                else:
                    raise
