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
import difflib
import unittest
from importlib import import_module
import pkgutil
import sys

import _environment  # noqa F401 # pylint: disable=unused-import
from test_base import TestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

# some tests only work with python2
if sys.version_info[0] < 3:
    EVENTDETECTOR_EXCLUDE = []
else:
    EVENTDETECTOR_EXCLUDE = ["test_inotifyx_events"]

PACKAGES = {
    "eventdetector": {
        "default": "TestEventDetector",
        "special": {
            "test_inotify_utils": "TestInotifyUtils"
        },
        "exclude": EVENTDETECTOR_EXCLUDE
    },
    "datafetcher": {
        "default": "TestDataFetcher",
        "special": {},
        "exclude": ["test_http_fetcher"]
    },
    "core": {
        "default": None,
        "special": {
            "test_taskprovider": "TestTaskProvider",
            "test_datadispatcher": "TestDataDispatcher",
            "test_signalhandler": "TestSignalHandler",
            "test_datamanager": "TestDataManager",
            "test_base_class": "TestBaseClass",
            "test_statserver": "TestStatServer",
        },
        "exclude": []
    },
    "api": {
        "default": None,
        "special": {
            "test_transfer": "TestTransfer",
            # "test_control": "TestControl",
            "test_control": "TestReceiverControl"
        },
        "exclude": []
    },
    "receiver": {
        "default": None,
        "special": {
            "test_datareceiver": "TestCheckNetgroup"
        },
        "exclude": []
    },
    "utils": {
        "default": None,
        "special": {
            "test_utils_config": "TestUtilsConfig"
        },
        "exclude": []
    }
}


def argument_parsing():
    """Parsing command line arguments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--suite",
                        type=str,
                        nargs="+",
                        help="Which test suites to test")

    parser.add_argument("--case",
                        type=str,
                        nargs="+",
                        help="Which test case to test individually "
                             "(mainly for debugging)")

    parser.add_argument("--debug",
                        action="store_true",
                        help="Start in debug mode")

    arguments = parser.parse_args()

    return arguments


def get_case_mapping(cases):
    """Get the unittest suites matching the specified case.

    Args:
        cases (list): A list of test cases to include.

    Returns:
        A list of test suites including the test classes.
    """

    all_suites = []

    for package_name in PACKAGES:
        modpath = import_module(package_name).__path__
        for _, modname, _ in pkgutil.iter_modules(modpath):

            if modname in cases:
                all_suites += get_suite(package_name, modname)

    return all_suites


def get_suite_mapping(suites):
    """Get the matching unittest suites sets by name.

    Args:
        suites (list): A list of suite set names.

    Returns:
        A list of unittest suites matching the given suite names.
    """

    all_suites = []

    for package_name in PACKAGES:
        if package_name in suites or "all" in suites:

            modpath = import_module(package_name).__path__
            for _, modname, _ in pkgutil.iter_modules(modpath):

                # the base class not a test module
                if (not modname.startswith("test")
                        or modname in PACKAGES[package_name]["exclude"]):
                    continue

                all_suites += get_suite(package_name, modname)

    return all_suites


def get_suite(package_name, name):
    """Get the unittestsuite by name.

    Args:
        package_name: The package the test modules belongs to.
        name: The name of the test module.

    Returns:
        A unittest suite corresponding to the specified module name.
    """

    module_name = "{}.{}".format(package_name, name)

    # just a more generic way of e.g. saying
    # from eventdetector.test_inotify_events import TestEventDetector
    module = import_module(module_name)
    if name in PACKAGES[package_name]["special"]:
        modclass = getattr(module, PACKAGES[package_name]["special"][name])
    else:
        if PACKAGES[package_name]["default"] is None:
            print("ERROR: Could not get suite. Maybe PACKAGES wrong "
                  "configuration?")
            return []

        modclass = getattr(module, PACKAGES[package_name]["default"])

    # load the test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(modclass)
    # this is equivalent to loading one module like this
    # > from eventdetector.test_inotifyx_events \
    # >     import TestEventDetector as TestInotifyxEvents
    # > loader = unittest.TestLoader()
    # > suite = loader.loadTestsFromTestCase(TestInotifyxEvents)

    return suite


def find_matches(namelist):
    """Find closest machtes in module names for given given strings.

    Args:
        namelist (list): A list of strings.

    Return:
        A dictionary with the closest matches for each string of the form:
            { <string1>: [<match1>, <match2>, ...], ...}
    """

    # all key in module (e.g. eventdetector)
    klist = []

    for package_name in PACKAGES:
        modpath = import_module(package_name).__path__
        # iter_modules returns: importer, modname, ispkg
        for _, modname, _ in pkgutil.iter_modules(modpath):
            klist.append(modname)

    res = {}
    for name in namelist:
        res[name] = difflib.get_close_matches(name, klist)

    return res


def main():
    """Run the test suite.
    """

    args = argument_parsing()

    if args.debug:
        TestBase.loglevel = "debug"

    # combine all sub-suites to one big one
    all_suites = []

    if args.suite is not None:
        all_suites += get_suite_mapping(args.suite)

    if args.case is not None:
        all_suites += get_case_mapping(args.case)

    if not all_suites:
        print("No tests found.")
        matches = find_matches(args.case)
        for key in matches:
            options = ", ".join(matches[key])
            if options:
                print("possible options for {} are: {}".format(key, options))
            else:
                print("No possible options found for {}".format(key))

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
