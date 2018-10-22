"""Testing the receiver.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import inspect
import json
import mock
import os
import threading
import time
import zmq

import utils
from .__init__ import BASE_DIR
from test_base import TestBase
import datareceiver
from datareceiver import CheckNetgroup, whitelist, changed_netgroup, reset_changed_netgroup

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestCheckNetgroup(TestBase):
    """Specification of tests to be performed for the receiver.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestCheckNetgroup, self).setUp()

    @mock.patch("utils.excecute_ldapsearch")
    @mock.patch("threading.Thread")
    def test_check_netgroup(self, mock_thread, mock_ldap):
        """Simulate netgroup changes.
        """

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
        self.log.info("{}: NO NETGROUP CHANGED".format(current_func_name))

        whitelist = ["test_host"]
        new_whitelist = whitelist
        mock_ldap.side_effect = [new_whitelist, MyException]

        checknetgroup = CheckNetgroup(**kwargs)
        reset_changed_netgroup()

        with mock.patch.object(datareceiver, "whitelist", whitelist):
            with self.assertRaises(MyException):
                checknetgroup.run()
            self.assertEqual(datareceiver.whitelist, whitelist)

        self.assertFalse(datareceiver.changed_netgroup)

        # --------------------------------------------------------------------
        # netgroup changed
        # --------------------------------------------------------------------
        self.log.info("{}: NETGROUP CHANGED".format(current_func_name))

        whitelist = ["test_host"]
        new_whitelist = ["new_test_host"]
        mock_ldap.side_effect = [new_whitelist, MyException]

        checknetgroup = CheckNetgroup(**kwargs)
        reset_changed_netgroup()

        with mock.patch.object(datareceiver, "whitelist", whitelist):
            with self.assertRaises(MyException):
                checknetgroup.run()
            self.assertEqual(datareceiver.whitelist, new_whitelist)

        self.assertTrue(datareceiver.changed_netgroup)

        # --------------------------------------------------------------------
        # empty ldap search
        # --------------------------------------------------------------------
        self.log.info("{}: EMPTY LDAP SEARCH".format(current_func_name))

        whitelist = ["test_host"]
        mock_ldap.side_effect = [[], whitelist, MyException]

        checknetgroup = CheckNetgroup(**kwargs)
        reset_changed_netgroup()

        with mock.patch.object(datareceiver, "whitelist", whitelist):
            with self.assertRaises(MyException):
                checknetgroup.run()
            self.assertEqual(datareceiver.whitelist, whitelist)

        self.assertFalse(datareceiver.changed_netgroup)

    def tearDown(self):
        super(TestCheckNetgroup, self).tearDown()
