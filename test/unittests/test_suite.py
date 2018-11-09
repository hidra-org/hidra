# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""Unittest suite for HiDRA
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import unittest
from importlib import import_module
import pkgutil

import _environment
import eventdetector
import datafetcher
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
    for _, modname, _ in pkgutil.iter_modules(eventdetector.__path__):
        # the base class not a test module
        if modname in ["eventdetector_test_base"]:
            continue

        # load the test suite
        module_name = "eventdetector.{}".format(modname)
        module = import_module(module_name).TestEventDetector
        suite = unittest.TestLoader().loadTestsFromTestCase(module)
        # this is equivalent to loading one module like this
        # > from eventdetector.test_inotifyx_events \
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
    for _, modname, _ in pkgutil.iter_modules(datafetcher.__path__):
        # the base class not a test module
        # TODO exclude test_http_fetcher only temporarily till bug is fixed
        if modname in ["datafetcher_test_base", "test_http_fetcher"]:
            continue

        # load the test suite
        module_name = "datafetcher.{}".format(modname)
        module = import_module(module_name).TestDataFetcher
        suite = unittest.TestLoader().loadTestsFromTestCase(module)
        # this is equivalent to loading one module like this
        # > from datafetcher.test_file_fetcher \
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
    """Invidual tests for manual testing.

    Returns:
        An array containing the added test suites.
    """

#    # pylint: disable=line-too-long
#
#    # for testing
#    from eventdetector.test_inotifyx_events import TestEventDetector as TestInotifyxEvents  # noqa F401
#    from eventdetector.test_watchdog_events import TestEventDetector as TestWatchdogEvents  # noqa F401
#    from eventdetector.test_http_events import TestEventDetector as TestHttpEvents  # noqa F401
#    from eventdetector.test_zmq_events import TestEventDetector as TestZmqEvents  # noqa F401
#    from eventdetector.test_hidra_events import TestEventDetector as TestHidraEvents  # noqa F401
#    from eventdetector.test_eventdetector_template import TestEventDetector as TestEventDetectorTemplate  # noqa F401
#
#    from datafetcher.test_cleanerbase import TestDataFetcher as TestCleanerbase  # noqa F401
#    from datafetcher.test_file_fetcher import TestDataFetcher as TestFileFetcher  # noqa F401
#    from datafetcher.test_http_fetcher import TestDataFetcher as TestHttpFetcher  # noqa F401
#    from datafetcher.test_zmq_fetcher import TestDataFetcher as TestZmqFetcher  # noqa F401
#    from datafetcher.test_zmq_fetcher import TestDataFetcher as TestZmqFetcher  # noqa F401
#    from datafetcher.test_hidra_fetcher import TestDataFetcher as TestHidraFetcher  # noqa F401
#    from datafetcher.test_datafetcher_template import TestDataFetcher as TestDataFetcherTemplate  # noqa F401
#
#    from api.test_control import TestControl  # noqa F401
#
    all_suites = []
#        unittest.TestLoader().loadTestsFromTestCase(TestInotifyxEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestWatchdogEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestHttpEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestZmqEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestHidraEvents),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestEventDetectorTemplate),  # noqa E122
#
#        unittest.TestLoader().loadTestsFromTestCase(TestCleanerbase),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestFileFetcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestHttpFetcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestZmqFetcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestHidraFetcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestDataFetcherTemplate),  # noqa E122
#
#        unittest.TestLoader().loadTestsFromTestCase(TestSignalHandler),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestTaskProvider),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestDataDispatcher),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestDataManager),  # noqa E122
#
#        unittest.TestLoader().loadTestsFromTestCase(TestTransfer),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestControl),  # noqa E122
#        unittest.TestLoader().loadTestsFromTestCase(TestReceiverControl),  # noqa E122
#
#        unittest.TestLoader().loadTestsFromTestCase(TestCheckNetgroup),  # noqa E122
#    ]

    return all_suites


def argument_parsing():
    """Parsing command line arguments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--suite",
                        type=str,
                        nargs="+",
                        required=True,
                        help="Which test suites to test")

    arguments = parser.parse_args()

    return arguments


def main():
    """Run the test suite.
    """

    args = argument_parsing()

    suite_args = args.suite

    all_suites = []

    if "api" in suite_args or "all" in suite_args:
        all_suites += get_api_suites()

    if "eventdetector" in suite_args or "all" in suite_args:
        all_suites += get_eventdetector_suites()

    if "datafetcher" in suite_args or "all" in suite_args:
        all_suites += get_datafetcher_suites()

    if "core" in suite_args or "all" in suite_args:
        all_suites += get_core_suites()

    if "receiver" in suite_args or "all" in suite_args:
        all_suites += get_receiver_suites()

    if "test" in suite_args:
        all_suites += get_testing_suites()

    # combine all subsuites to one big one
    suite = unittest.TestSuite(all_suites)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)


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
