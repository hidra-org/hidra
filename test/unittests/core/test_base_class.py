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

"""Testing the task provider.
"""

# pylint: disable=missing-docstring
# pylint: disable=protected-access
# pylint: disable=redefined-variable-type

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from multiprocessing import freeze_support

try:
    import unittest.mock as mock
except ImportError:
    # for python2
    import mock

from test_base import TestBase
from base_class import Base
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestBaseClass(TestBase):
    """Specification of tests to be performed for the base class.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestBaseClass, self).setUp()

        # see https://docs.python.org/2/library/multiprocessing.html#windows
        freeze_support()

    @mock.patch("base_class.Base._check_config_base")
    def test__base_check(self, mock_check):
        """Simulate incoming data and check if received events are correct.
        """

        obj = Base()

        module_param = {"param1": 1, "param2": 2}
        obj.config_all = {
            "eventdetector": {
                "type": "my_module",
                "my_module": module_param
            }
        }

        # --------------------------------------------------------------------
        # not checking dependend parameter
        # --------------------------------------------------------------------
        expected_result = {"eventdetector": {"type": "my_module"}}
        obj.required_params_base = {"eventdetector": ["type"]}
        mock_check.return_value = expected_result

        obj._base_check("eventdetector", check_dep=False)
        self.assertEqual(obj.required_params_dep, {})
        self.assertEqual(obj.config_reduced, expected_result)

        mock_check.assert_called()

        # --------------------------------------------------------------------
        # checking dependend parameter
        # --------------------------------------------------------------------

        expected_result = {"eventdetector": {"my_module": module_param}}
        mock_check.side_effect = [{}, expected_result]

        obj._base_check("eventdetector", check_dep=True)
        self.assertEqual(obj.required_params_dep,
                         {"eventdetector": ["my_module"]})
        self.assertEqual(obj.config_reduced, expected_result)

    @mock.patch("hidra.utils.check_config")
    def test_check_config_base(self, mock_check):
        obj = Base()

        # --------------------------------------------------------------------
        # wrong input parameter
        # --------------------------------------------------------------------
        with self.assertRaises(utils.WrongConfiguration):
            obj._check_config_base(config={}, required_params=1)

        # --------------------------------------------------------------------
        # config ok, no param
        # --------------------------------------------------------------------
        with mock.patch("hidra.utils.check_config") as mock_check:
                                     # (check_passed, config_part)

            expected_result = {"test": "successfull"}
            mock_check.return_value = (True, expected_result)

            ret_val = obj._check_config_base(config={}, required_params=[])

            self.assertFalse(mock_check.called)
            self.assertEqual(ret_val, {})

        # --------------------------------------------------------------------
        # config ok
        # --------------------------------------------------------------------
        with mock.patch("hidra.utils.check_config") as mock_check:
                                     # (check_passed, config_part)
            expected_result = {"test": "successfull"}
            mock_check.return_value = (True, expected_result)

            ret_val = obj._check_config_base(config={}, required_params=["test_param"])

            mock_check.assert_called()
            self.assertEqual(ret_val, expected_result)

        # --------------------------------------------------------------------
        # wrong config
        # --------------------------------------------------------------------
        with mock.patch("hidra.utils.check_config") as mock_check:
                                     # (check_passed, config_part)
            mock_check.return_value = (False, {})

            with self.assertRaises(utils.WrongConfiguration):
                obj._check_config_base(config={}, required_params=["test_param"])

    def test_start_socket(self):  # pylint: disable=no-self-use
        # TODO
        pass

    def test_stop_socket(self):
        obj = Base()

        # --------------------------------------------------------------------
        # throw AttributeError
        # --------------------------------------------------------------------
        with mock.patch("hidra.utils.stop_socket") as mock_stop:
            ret_val = obj.stop_socket(name="a", socket=None)
            self.assertIsNone(ret_val)

        # --------------------------------------------------------------------
        # stop internal socket
        # --------------------------------------------------------------------
        with mock.patch("hidra.utils.stop_socket") as mock_stop:

            obj.test_socket_tmp = None
            expected_result = "test_value"
            mock_stop.return_value = expected_result

            ret_val = obj.stop_socket(name="test_socket_tmp", socket=None)

            mock_stop.assert_called()
            self.assertIsNone(ret_val)
            self.assertEqual(obj.test_socket_tmp, expected_result)

            del obj.test_socket_tmp

        # --------------------------------------------------------------------
        # stop external socket
        # --------------------------------------------------------------------
        with mock.patch("hidra.utils.stop_socket") as mock_stop:
            expected_result = "test_value"
            mock_stop.return_value = expected_result

            ret_val = obj.stop_socket(name="test_sckt", socket="some_socket")

            mock_stop.assert_called()
            self.assertEqual(ret_val, expected_result)

    @mock.patch("base_class.Base._forward_control_signal")
    @mock.patch("base_class.Base._react_to_exit_signal")
    @mock.patch("base_class.Base._react_to_close_sockets_signal")
    @mock.patch("base_class.Base._react_to_sleep_signal")
    @mock.patch("base_class.Base._react_to_wakeup_signal")
    def test_check_control_signal(self,
                                  mock_wakeup,
                                  mock_sleep,
                                  mock_close,
                                  mock_exit,
                                  mock_forward):
        obj = Base()
        obj.control_socket = mock.MagicMock()
        obj.log = mock.MagicMock()

        def _reset_mock():
            mock_forward.reset_mock()
            mock_exit.reset_mock()
            mock_close.reset_mock()
            mock_sleep.reset_mock()
            mock_wakeup.reset_mock()

        # --------------------------------------------------------------------
        # receiving failed
        # --------------------------------------------------------------------
        obj.control_socket.recv_multipart.side_effect = [Exception()]
        ret_val = obj.check_control_signal()
        self.assertFalse(ret_val)
        obj.log.error.assert_called()

        obj.control_socket.reset_mock()

        # --------------------------------------------------------------------
        # received exit
        # --------------------------------------------------------------------
        _reset_mock()
        obj.control_socket.recv_multipart.side_effect = [["topic", b"EXIT"]]

        ret_val = obj.check_control_signal()

        mock_forward.assert_called()
        mock_exit.assert_called()
        self.assertFalse(mock_close.called)
        self.assertFalse(mock_sleep.called)
        self.assertFalse(mock_wakeup.called)
        self.assertTrue(ret_val)

        # --------------------------------------------------------------------
        # received close_sockets
        # --------------------------------------------------------------------
        _reset_mock()
        obj.control_socket.recv_multipart.side_effect = [["topic",
                                                          b"CLOSE_SOCKETS"]]

        ret_val = obj.check_control_signal()

        mock_forward.assert_called()
        self.assertFalse(mock_exit.called)
        mock_close.assert_called()
        self.assertFalse(mock_sleep.called)
        self.assertFalse(mock_wakeup.called)
        self.assertFalse(ret_val)

        # --------------------------------------------------------------------
        # received sleep
        # --------------------------------------------------------------------
        _reset_mock()
        obj.control_socket.recv_multipart.side_effect = [["topic", b"SLEEP"]]

        ret_val = obj.check_control_signal()

        mock_forward.assert_called()
        self.assertFalse(mock_exit.called)
        self.assertFalse(mock_close.called)
        mock_sleep.assert_called()
        self.assertFalse(mock_wakeup.called)
        self.assertFalse(ret_val)

        # --------------------------------------------------------------------
        # received wakeup
        # --------------------------------------------------------------------
        _reset_mock()
        obj.control_socket.recv_multipart.side_effect = [["topic", b"WAKEUP"]]

        ret_val = obj.check_control_signal()

        mock_forward.assert_called()
        self.assertFalse(mock_exit.called)
        self.assertFalse(mock_close.called)
        self.assertFalse(mock_sleep.called)
        mock_wakeup.assert_called()
        self.assertFalse(ret_val)

        # --------------------------------------------------------------------
        # unhandled control signal
        # --------------------------------------------------------------------
        obj.control_socket.recv_multipart.side_effect = [["topic", b"blubb"]]

        ret_val = obj.check_control_signal()

        self.assertFalse(ret_val)
        obj.log.error.assert_called()

    def test__react_to_sleep_signal(self):  # pylint: disable=no-self-use
        pass
        # TODO

    def tearDown(self):
        super(TestBaseClass, self).tearDown()
