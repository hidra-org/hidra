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

"""Testing the zmq_events event detector.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

try:
    import unittest.mock as mock
except ImportError:
    # for python2
    import mock
import zmq

from test_base import TestBase, MockZmqSocket, MockZmqPoller
import hidra.control as m_control

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestException(Exception):
    """A custom exception to throw and catch.
    """
    pass


class TestReceiverControl(TestBase):
    """Specification of tests to be performed for the control API.
    """

    def setUp(self):
        super().setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        self.host = self.con_ip
        self.port = 1234

        with mock.patch("zmq.Context"):
            with mock.patch("zmq.Poller"):
                with mock.patch("hidra.control.ReceiverControl._start_socket"):
                    self.control = m_control.ReceiverControl(self.host,
                                                             self.port)

    def test__setup(self):
        """Test the setup method.
        """

        with mock.patch("hidra.control.ReceiverControl._setup"):
            self.control = m_control.ReceiverControl(self.host, self.port)

        # pylint: disable=protected-access
        self.control._setup(self.host, self.port)

        self.assertIsInstance(self.control.context, zmq.Context)
        self.assertIsInstance(self.control.status_socket,
                              zmq.sugar.socket.Socket)
        self.assertIsInstance(self.control.poller, zmq.Poller)

    def test_get_response(self):
        """Test the get_response method.
        """

        # pylint: disable=protected-access

        self.control.status_socket = MockZmqSocket()
        self.control.status_socket.recv_multipart.return_value = [
            "test_response"
        ]

        self.control.poller = MockZmqPoller()

        # --------------------------------------------------------------------
        # OK
        # --------------------------------------------------------------------
        self.control.poller.poll.return_value = {
            self.control.status_socket: zmq.POLLIN
        }
        ret_val = self.control._get_response()

        self.assertEqual(ret_val, ["test_response"])

        # --------------------------------------------------------------------
        # Exception in polling
        # --------------------------------------------------------------------
        self.control.poller.poll.side_effect = TestException()

        with self.assertRaises(TestException):
            self.control._get_response()

        self.control.poller.poll.side_effect = None

        # --------------------------------------------------------------------
        # timeout
        # --------------------------------------------------------------------
        self.control.poller.poll.return_value = {}

        with self.assertRaises(m_control.CommunicationFailed):
            ret_val = self.control._get_response()

        # cleanup
        self.control.status_socket = None

    def test_get_status(self):
        """Test the get_status method.
        """

        # pylint: disable=protected-access

        self.control._get_response = MockZmqSocket()
        self.control._get_response.return_value = ["test_response"]

        self.control.status_socket = MockZmqSocket()

        ret_val = self.control.get_status()

        self.assertEqual(ret_val, ["test_response"])
        self.control.status_socket.send_multipart.assert_called_once_with(
            [b"STATUS_CHECK"]
        )
        self.control._get_response.assert_called_once_with()

        # cleanup
        self.control.status_socket = None

    def test_reset_status(self):
        """Test the reset_status method.
        """

        # pylint: disable=protected-access

        self.control._get_response = MockZmqSocket()
        self.control._get_response.return_value = ["test_response"]

        self.control.status_socket = MockZmqSocket()

        self.control.reset_status()

        self.control.status_socket.send_multipart.assert_called_once_with(
            [b"RESET_STATUS"]
        )
        self.control._get_response.assert_called_once_with()

        # cleanup
        self.control.status_socket = None

    def tearDown(self):
        super().tearDown()

        self.control.stop()
