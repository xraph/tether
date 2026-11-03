"""Run one Eclipse Paho interoperability test module against a broker.

Run it as `python3 -I run_suite.py ...`. Isolated mode keeps the working
directory and the user site off sys.path, so this script adds the suite's
interoperability directory itself. The suite is third-party code: keep it in
its own directory outside this repository and never run it from inside that
directory.

The suite's own `__main__` blocks read the host and port from argv in
different ways (client_test.py leaves them in argv for unittest to trip over).
This runner imports the module, sets the globals those blocks would set, and
runs unittest itself, with a per-test timeout and a summary line per test:

    RESULT <ok|FAIL|ERROR|skip> <test name>
"""

import argparse
import importlib
import logging
import signal
import sys
import time
import unittest


class TestTimeout(Exception):
    pass


def on_alarm(signum, frame):
    raise TestTimeout("test timed out")


class Recorder(unittest.TextTestResult):
    """Collects one status per test and enforces the per-test timeout."""

    timeout = 120

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.statuses = {}

    def startTest(self, test):
        signal.alarm(self.timeout)
        super().startTest(test)

    def stopTest(self, test):
        signal.alarm(0)
        super().stopTest(test)

    def addSuccess(self, test):
        self.statuses[test.id()] = "ok"
        super().addSuccess(test)

    def addFailure(self, test, err):
        self.statuses[test.id()] = "FAIL"
        super().addFailure(test, err)

    def addError(self, test, err):
        status = "TIMEOUT" if issubclass(err[0], TestTimeout) else "ERROR"
        self.statuses[test.id()] = status
        super().addError(test, err)

    def addSkip(self, test, reason):
        self.statuses[test.id()] = "skip"
        super().addSkip(test, reason)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--suite", required=True, help="path to a paho.mqtt.testing checkout")
    p.add_argument("--module", required=True, choices=["client_test", "client_test5"])
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--timeout", type=int, default=120, help="seconds per test")
    p.add_argument("--only", action="append", default=[], help="run only this test method (repeatable)")
    args = p.parse_args()

    sys.path.insert(0, args.suite.rstrip("/") + "/interoperability")
    sys.argv = [args.module]
    logging.getLogger().setLevel(logging.ERROR)

    mod = importlib.import_module(args.module)
    mod.host = args.host
    mod.port = args.port
    mod.iterations = 1
    mod.nosubscribe_topic_filter = "test/nosubscribe"
    mod.nosubscribe_topics = ("test/nosubscribe",)

    if args.module == "client_test5":
        mod.topic_prefix = "client_test5/"
        mod.topics = [mod.topic_prefix + t for t in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
        mod.wildtopics = [mod.topic_prefix + t for t in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]
    else:
        mod.topics = ("TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA")
        mod.wildtopics = ("TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#")

    Recorder.timeout = args.timeout
    signal.signal(signal.SIGALRM, on_alarm)

    loader = unittest.TestLoader()
    if args.only:
        suite = unittest.TestSuite(loader.loadTestsFromName(n, mod.Test) for n in args.only)
    else:
        suite = loader.loadTestsFromTestCase(mod.Test)

    runner = unittest.TextTestRunner(stream=sys.stdout, verbosity=2, resultclass=Recorder)
    start = time.time()
    result = runner.run(suite)
    print("\nelapsed %.1fs" % (time.time() - start))

    for name, status in sorted(result.statuses.items()):
        print("RESULT %s %s" % (status, name.split(".")[-1]))

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
