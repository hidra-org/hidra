"""Testing the zmq_events event detector.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import inspect
import mock
import zmq

from .__init__ import BASE_DIR
from test_base import TestBase, MockZmqSocket, MockZmqPoller
import hidra
import hidra.control as m_control

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

class TestException(Exception):
    pass

class TestControl(TestBase):
    """
    """

    def setUp(self):
        super(TestControl, self).setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

    def old_test_receiver(self):
        with mock.patch("zmq.Context") as mock_context:
            mock_context.return_value = mock.MagicMock(socket=mock.MagicMock())
            mock_socket = mock.MagicMock()

            m_control.reset_receiver_status("test_host", 50050)

            print(mock_context.called)
            print(mock_context.mock_calls)
            print(mock_context.socket.send_multipart.called)

    def tearDown(self):
        super(TestControl, self).tearDown()


class TestReceiverControl(TestBase):
    """
    """
    def setUp(self):
        super(TestReceiverControl, self).setUp()

        self.host = self.con_ip
        self.port = 1234

        with mock.patch("zmq.Context"):
            with mock.patch("zmq.Poller"):
                with mock.patch("hidra.control.ReceiverControl._start_socket"):
                    self.control = m_control.ReceiverControl(self.host, self.port)

    def test__setup(self):
        with mock.patch("hidra.control.ReceiverControl._setup"):
            self.control = m_control.ReceiverControl(self.host, self.port)

        self.control._setup(self.host, self.port)

        self.assertIsInstance(self.control.context, zmq.Context)
        self.assertIsInstance(self.control.status_socket, zmq.sugar.socket.Socket)
        self.assertIsInstance(self.control.poller, zmq.Poller)

    def test_get_response(self):

        self.control.status_socket = MockZmqSocket()
        self.control.status_socket.recv_multipart.return_value = ["test_response"]

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
        self.control._get_response = MockZmqSocket()
        self.control._get_response.return_value = ["test_response"]

        self.control.status_socket = MockZmqSocket()

        ret_val = self.control.get_status()

        self.assertEqual(ret_val, ["test_response"])
        self.control.status_socket.send_multipart.assert_called_once_with(
            ["STATUS_CHECK"]
        )
        self.control._get_response.assert_called_once_with()

        # cleanup
        self.control.status_socket = None

    def test_reset_status(self):
        self.control._get_response = MockZmqSocket()
        self.control._get_response.return_value = ["test_response"]

        self.control.status_socket = MockZmqSocket()

        self.control.reset_status()

        self.control.status_socket.send_multipart.assert_called_once_with(
            ["RESET_STATUS"]
        )
        self.control._get_response.assert_called_once_with()

        # cleanup
        self.control.status_socket = None

    def tearDown(self):
        super(TestReceiverControl, self).tearDown()

        self.control.stop()
