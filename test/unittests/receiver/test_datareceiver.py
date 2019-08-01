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

"""Testing the receiver.
"""

# pylint: disable=protected-access
# pylint: disable=missing-docstring

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import inspect
import threading

try:
    import unittest.mock as mock
except ImportError:
    # for python2
    import mock

from test_base import TestBase
import datareceiver
from datareceiver import CheckNetgroup, reset_changed_netgroup

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestCheckNetgroup(TestBase):
    """Specification of tests to be performed for the receiver.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super().setUp()

    @mock.patch("hidra.utils.execute_ldapsearch")
    @mock.patch("threading.Thread")
    def test_check_netgroup(self, mock_thread, mock_ldap):
        """Simulate netgroup changes.
        """
        # pylint: disable=unused-argument

        current_func_name = inspect.currentframe().f_code.co_name

        # to stop loop
        class MyException(Exception):
            pass

        kwargs = dict(
            netgroup="test_netgroup",
            lock=threading.Lock(),
            ldapuri="test_ldapuri",
            ldap_retry_time=0.1,
            check_time=0.1
        )

        # --------------------------------------------------------------------
        # no netgroup change
        # --------------------------------------------------------------------
        self.log.info("%s: NO NETGROUP CHANGED", current_func_name)

        whitelist = ["test_host"]
        new_whitelist = whitelist
        mock_ldap.side_effect = [new_whitelist, MyException]

        checknetgroup = CheckNetgroup(**kwargs)
        reset_changed_netgroup()

        with mock.patch.object(datareceiver, "_whitelist", whitelist):
            with self.assertRaises(MyException):
                checknetgroup.run()
            self.assertEqual(datareceiver._whitelist, whitelist)

        self.assertFalse(datareceiver._changed_netgroup)

        # --------------------------------------------------------------------
        # netgroup changed
        # --------------------------------------------------------------------
        self.log.info("%s: NETGROUP CHANGED", current_func_name)

        whitelist = ["test_host"]
        new_whitelist = ["new_test_host"]
        mock_ldap.side_effect = [new_whitelist, MyException]

        checknetgroup = CheckNetgroup(**kwargs)
        reset_changed_netgroup()

        with mock.patch.object(datareceiver, "_whitelist", whitelist):
            with self.assertRaises(MyException):
                checknetgroup.run()
            self.assertEqual(datareceiver._whitelist, new_whitelist)

        self.assertTrue(datareceiver._changed_netgroup)

        # --------------------------------------------------------------------
        # empty ldap search
        # --------------------------------------------------------------------
        self.log.info("%s: EMPTY LDAP SEARCH", current_func_name)

        whitelist = ["test_host"]
        mock_ldap.side_effect = [[], whitelist, MyException]

        checknetgroup = CheckNetgroup(**kwargs)
        reset_changed_netgroup()

        with mock.patch.object(datareceiver, "_whitelist", whitelist):
            with self.assertRaises(MyException):
                checknetgroup.run()
            self.assertEqual(datareceiver._whitelist, whitelist)

        self.assertFalse(datareceiver._changed_netgroup)
