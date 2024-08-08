import logging
import re

from ingestion.utils import Stopwatch, report_generator_time, report_time

REPORT_REGEX = re.compile(
    r"TIMING: .*"
    r"start_time=\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}, "
    r"end_time=\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}, "
    r"elapsed_time=0:00:\d\d",
)


def test_stopwatch_generates_a_report(caplog):
    caplog.set_level(logging.INFO)
    s = Stopwatch()
    s.start()
    s.stop()
    s.report()

    messages = [r.message for r in caplog.records]
    assert len(messages) == 1
    assert re.match(
        REPORT_REGEX,
        messages[0],
    )


def test_report_time_generates_a_report(caplog):
    caplog.set_level(logging.INFO)

    @report_time
    def foo():
        return 1 + 1

    assert foo() == 2

    messages = [r.message for r in caplog.records]
    assert len(messages) == 1
    assert re.match(
        REPORT_REGEX,
        messages[0],
    )
    assert "function=foo, " in messages[0]


def test_report_generate_time(caplog):
    caplog.set_level(logging.INFO)

    @report_generator_time
    def foo():
        yield 1
        yield 2

    generator = foo()
    values = list(generator)
    assert values == [1, 2]

    messages = [r.message for r in caplog.records]
    assert len(messages) == 1
    assert re.match(
        REPORT_REGEX,
        messages[0],
    )
    assert "function=foo, " in messages[0]
