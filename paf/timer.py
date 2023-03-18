import collections
import time


Timer = collections.namedtuple("Timer", "handler expiration_time")


class TimerManager:
    def __init__(self, next_timeout_cb=None):
        self.next_timeout_cb = next_timeout_cb
        self.deque = collections.deque()

    def empty(self):
        return len(self.deque) == 0

    def add(self, handler, expiration_time, relative=False):
        if relative:
            expiration_time += time.time()

        new_timer = Timer(handler, expiration_time)

        idx = self._allocate_idx(expiration_time)

        self.deque.insert(idx, new_timer)

        if idx == 0:
            self.next_timeout_changed()

        return new_timer

    def remove(self, timer):
        if timer is self.deque[0]:
            self.deque.popleft()
            self.next_timeout_changed()
        else:
            self.deque.remove(timer)

    def next_timeout(self):
        if self.empty():
            return None

        return self.deque[0].expiration_time

    def process(self):
        now = time.time()

        changed = False

        while len(self.deque) > 0:
            candidate = self.deque[0]

            if now < candidate.expiration_time:
                break

            self.deque.popleft()
            candidate.handler()

            changed = True

        if changed:
            self.next_timeout_changed()

    def next_timeout_changed(self):
        if self.next_timeout_cb is not None:
            self.next_timeout_cb()

    def _allocate_idx(self, expiration_time):
        # Python deque does not provide O(1) indexed access, but its
        # implemention better than a naive linked list, and to do a
        # binary search seemingly works pretty well, and certainly
        # much better than to do a linear search.

        if len(self.deque) == 0:
            return 0

        low = 0
        high = len(self.deque)

        # Special case for the most common case: adding timers last in
        # the list.
        if expiration_time > self.deque[high - 1].expiration_time:
            return high

        while True:
            idx = (low + high) // 2

            if (high - low) <= 1:
                break

            if expiration_time > self.deque[idx].expiration_time:
                low = idx + 1
            elif expiration_time < self.deque[idx].expiration_time:
                high = idx
            else:
                break

        if expiration_time > self.deque[idx].expiration_time:
            idx += 1

        return idx

    def __iter__(self):
        return iter(self.deque)
