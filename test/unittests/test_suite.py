from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import unittest
from importlib import import_module
import pkgutil

import event_detectors


def get_eventdetector_suites():
    """Collects all available eventdetector tests

    Returns:
        An array containing all available eventdetector test suites.
    """

    all_suites = []

    event_detectors_m = "event_detectors"
    # find all event detector test modules
    # iter_modules returns: importer, modname, ispkg
    for _, modname, _ in pkgutil.iter_modules(event_detectors.__path__):
        # the base class not a test module
        if modname == "test_eventdetector_base":
            continue

        # load the test suite
        module_name = "{}.{}".format(event_detectors_m, modname)
        module = import_module(module_name).TestEventDetector
        suite = unittest.TestLoader().loadTestsFromTestCase(module)
        # this is equivalent to loading one module like this
        # > from event_detectors.test_inotifyx_events \
        # >     import TestEventDetector as TestInotifyxEvents
        # > loader = unittest.TestLoader()
        # > suite = loader.loadTestsFromTestCase(TestInotifyxEvents)

        # add the test suite
        all_suites.append(suite)

    return all_suites


def suite():
    """ Collect all tests to be performed as one suite.

    Returns:
        A unittest TestSuite instance containing all unittests to be performed.
    """

    # get the subsuites
    all_suites = []
    all_suites += get_eventdetector_suites()

    # combine all subsuites to one big one
    suite = unittest.TestSuite(all_suites)

    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
