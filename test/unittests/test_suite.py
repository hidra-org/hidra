"""Unittest suite for HiDRA
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import unittest
from importlib import import_module
import pkgutil

import eventdetectors
import datafetchers
import api
import receiver
from core.test_taskprovider import TestTaskProvider
from core.test_datadispatcher import TestDataDispatcher
from core.test_signalhandler import TestSignalHandler
from core.test_datamanager import TestDataManager

from api.test_transfer import TestTransfer
from api.test_control import TestReceiverControl

from receiver.test_datareceiver import TestCheckNetgroup

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def get_eventdetector_suites():
    """Collects all available eventdetector tests

    Returns:
        An array containing all available eventdetector test suites.
    """

    all_suites = []

    # find all event detector test modules
    # iter_modules returns: importer, modname, ispkg
    for _, modname, _ in pkgutil.iter_modules(eventdetectors.__path__):
        # the base class not a test module
        if modname in ["eventdetector_test_base"]:
            continue

        # load the test suite
        module_name = "eventdetectors.{}".format(modname)
        module = import_module(module_name).TestEventDetector
        suite = unittest.TestLoader().loadTestsFromTestCase(module)
        # this is equivalent to loading one module like this
        # > from eventdetectors.test_inotifyx_events \
        # >     import TestEventDetector as TestInotifyxEvents
        # > loader = unittest.TestLoader()
        # > suite = loader.loadTestsFromTestCase(TestInotifyxEvents)

        # add the test suite
        all_suites.append(suite)

    return all_suites


def get_datafetcher_suites():
    """Collects all available datafetcher tests

    Returns:
        An array containing all available datafetcher test suites.
    """

    all_suites = []

    # find all event detector test modules
    # iter_modules returns: importer, modname, ispkg
    for _, modname, _ in pkgutil.iter_modules(datafetchers.__path__):
        # the base class not a test module
        # TODO exclude test_http_fetcher only temporarily till bug is fixed
        if modname in ["datafetcher_test_base", "test_http_fetcher"]:
            continue

        # load the test suite
        module_name = "datafetchers.{}".format(modname)
        module = import_module(module_name).TestDataFetcher
        suite = unittest.TestLoader().loadTestsFromTestCase(module)
        # this is equivalent to loading one module like this
        # > from datafetchers.test_file_fetcher \
        # >     import TestDataFetcher as TestFileFetcher
        # > loader = unittest.TestLoader()
        # > suite = loader.loadTestsFromTestCase(TestFileFetcher)

        # add the test suite
        all_suites.append(suite)

    return all_suites


def get_core_suites():
    """Collects all available hidra core tests

    Returns:
        An array containing all available core test suites.
    """

    all_suites = [
        unittest.TestLoader().loadTestsFromTestCase(TestTaskProvider),  # noqa E122
        unittest.TestLoader().loadTestsFromTestCase(TestDataDispatcher),  # noqa E122
        unittest.TestLoader().loadTestsFromTestCase(TestDataManager),  # noqa E122
        unittest.TestLoader().loadTestsFromTestCase(TestSignalHandler),  # noqa E122
    ]

    return all_suites


def get_api_suites():
    """Collects all available hidra api tests

    Returns:
        An array containing all available api test suites.
    """

    all_suites = [
        unittest.TestLoader().loadTestsFromTestCase(TestTransfer),  # noqa E122
        unittest.TestLoader().loadTestsFromTestCase(TestReceiverControl),  # noqa E122
    ]

    return all_suites

def get_receiver_suites():
    """Collects all available receiver tests

    Returns:
        An array containing all available receiver test suites.
    """

    all_suites = [
        unittest.TestLoader().loadTestsFromTestCase(TestCheckNetgroup),  # noqa E122
    ]

    return all_suites

def get_testing_suites():

    # for testing
    from eventdetectors.test_inotifyx_events import TestEventDetector as TestInotifyxEvents  # noqa F401
    from eventdetectors.test_watchdog_events import TestEventDetector as TestWatchdogEvents  # noqa F401
    from eventdetectors.test_http_events import TestEventDetector as TestHttpEvents  # noqa F401
    from eventdetectors.test_zmq_events import TestEventDetector as TestZmqEvents  # noqa F401
    from eventdetectors.test_hidra_events import TestEventDetector as TestHidraEvents  # noqa F401
    from eventdetectors.test_eventdetector_template import TestEventDetector as TestEventDetectorTemplate  # noqa F401

    from datafetchers.test_cleanerbase import TestDataFetcher as TestCleanerbase  # noqa F401
    from datafetchers.test_file_fetcher import TestDataFetcher as TestFileFetcher  # noqa F401
    from datafetchers.test_http_fetcher import TestDataFetcher as TestHttpFetcher  # noqa F401
    from datafetchers.test_zmq_fetcher import TestDataFetcher as TestZmqFetcher  # noqa F401
    from datafetchers.test_zmq_fetcher import TestDataFetcher as TestZmqFetcher  # noqa F401
    from datafetchers.test_hidra_fetcher import TestDataFetcher as TestHidraFetcher  # noqa F401
    from datafetchers.test_datafetcher_template import TestDataFetcher as TestDataFetcherTemplate  # noqa F401

    from api.test_control import TestControl

    all_suites = [
#        unittest.TestLoader().loadTestsFromTestCase(TestInotifyxEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestWatchdogEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestHttpEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestZmqEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestHidraEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestEventDetectorTemplate),  # noqa E122

#        unittest.TestLoader().loadTestsFromTestCase(TestCleanerbase),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestFileFetcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestHttpFetcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestZmqFetcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestHidraFetcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestDataFetcherTemplate),  # noqa E122

#        unittest.TestLoader().loadTestsFromTestCase(TestSignalHandler),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestTaskProvider),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestDataDispatcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestDataManager),  # noqa E122

#        unittest.TestLoader().loadTestsFromTestCase(TestTransfer),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestControl),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestReceiverControl),  # noqa E122

        unittest.TestLoader().loadTestsFromTestCase(TestCheckNetgroup),  # noqa E122
    ]

    return all_suites


def get_suite():
    """Collect all tests to be performed as one suite.

    Returns:
        A unittest TestSuite instance containing all unittests to be performed.
    """

    all_suites = []

    # get the subsuites
    all_suites += get_api_suites()
    all_suites += get_eventdetector_suites()
    all_suites += get_datafetcher_suites()
    all_suites += get_core_suites()
    all_suites += get_receiver_suites()

#    all_suites += get_testing_suites()

    # combine all subsuites to one big one
    suite = unittest.TestSuite(all_suites)

    return suite


def main():
    """Run the test suite.
    """

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(get_suite())


if __name__ == '__main__':
    main()

#    import pprint
#    import threading

#    from hanging_threads import start_monitoring
#    monitoring_thread = start_monitoring(seconds_frozen=30, test_interval=10)

#    import pdb
#    pdb.run('main()', globals(), locals())

#    print("At the end")
#    pprint.pprint(threading._active)
