import logging
import time
from datetime import timedelta

logging.basicConfig(level=logging.DEBUG)


def report_time(func):
    """
    Decorator to report the total time of a function call
    """

    def wrapped_func(*args, **kwargs):
        arg_types = [type(arg) for arg in args]
        stopwatch = Stopwatch(
            function=func.__name__, arg_types=arg_types, kwargs=kwargs
        )

        stopwatch.start()

        r = func(*args, **kwargs)

        stopwatch.stop()
        stopwatch.report()

        return r

    return wrapped_func


def report_generator_time(func):
    """
    Decorator to report the total time of an iterable
    """

    def wrapped_func(*args, **kwargs):
        arg_types = [type(arg) for arg in args]
        stopwatch = Stopwatch(
            function=func.__name__, arg_types=arg_types, kwargs=kwargs
        )

        stopwatch.start()

        r = func(*args, **kwargs)
        yield from r

        stopwatch.stop()
        stopwatch.report()

        return r

    return wrapped_func


class Stopwatch:
    """
    Wrapper around the time module for timing code execution
    """

    def __init__(self, **meta):
        self.running = False
        self.start_time = None
        self.stop_time = None
        self.elapsed = 0
        joined_meta = ", ".join(f"{k}={v}" for k, v in meta.items())
        self.prefix = f"TIMING: {joined_meta}, " if joined_meta else "TIMING: "

    def start(self):
        self.start_time = time.time()
        self.running = True

    def stop(self):
        self.running = False
        if not self.start_time:
            return

        now = time.time()
        elapsed = now - self.start_time
        self.stop_time = now
        self.elapsed += elapsed

    def report(self):
        logging.info(
            f"{self.prefix}"
            f"start_time={time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self.start_time))}, "
            f"end_time={time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self.stop_time))}, "
            f"elapsed_time={str(timedelta(seconds=self.elapsed))}"
        )
