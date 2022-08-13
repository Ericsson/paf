import time
import random

# roughly the rate of a modern x86_64 desktop CPU
BASE_LINE_OPS_RATE = 20e6
BENCHMARK_OPS = 1000000


def determine_relative_speed():
    start = time.time()

    for i in range(BENCHMARK_OPS):
        random.random()

    latency = time.time() - start

    rate = BENCHMARK_OPS / latency

    relative_speed = rate / BASE_LINE_OPS_RATE

    return relative_speed


_relative_speed = None


def relative_speed():
    global _relative_speed

    if _relative_speed is None:
        _relative_speed = determine_relative_speed()

    return _relative_speed


def adjust(num):
    speed = relative_speed()

    adjusted_num = num * speed

    if isinstance(num, int):
        adjusted_num = int(adjusted_num)

    return adjusted_num


def adjust_down(num):
    adjusted_num = adjust(num)

    return min(num, adjusted_num)
