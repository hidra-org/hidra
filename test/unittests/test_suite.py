import unittest

from event_detectors.test_inotityx_events import TestEventDetector as TestInotifyxEvents
from event_detectors.test_watchdog_events import TestEventDetector as TestWatchdogEvents
from event_detectors.test_http_events import TestEventDetector as TestHttpEvents

def suite():

    all_suites = [
        unittest.TestLoader().loadTestsFromTestCase(TestInotifyxEvents),
        unittest.TestLoader().loadTestsFromTestCase(TestWatchdogEvents),
        unittest.TestLoader().loadTestsFromTestCase(TestHttpEvents)
    ]
    suite = unittest.TestSuite(all_suites)

    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())

