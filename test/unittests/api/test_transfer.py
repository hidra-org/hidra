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

# pylint: disable=protected-access
# pylint: disable=missing-docstring
# pylint: disable=redefined-variable-type

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import errno
import inspect
import json
import logging
from multiprocessing import Queue
import re
import socket
import zmq

import mock

#from .__init__ import BASE_DIR
from test_base import (TestBase,
                       MockZmqSocket,
                       MockZmqPollerAllFake,
                       MockZmqAuthenticator)
import hidra
import hidra.transfer as m_transfer
from hidra._version import __version__

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestException(Exception):
    """A custom exception to throw and catch.
    """
    pass


class TestTransfer(TestBase):
    """Specification of tests to be performed for the transfer API.
    """

    def setUp(self):
        super(TestTransfer, self).setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        self.transfer_conf = dict(
            connection_type="STREAM",
            signal_host=None,
            use_log=False,
            context="fake_context",
            dirs_not_to_create=None
        )


    def test_get_logger(self):

        def test_logger_with_queue(log_level, logging_level):
            """Helper function to test logger using a log queue.
            """

            queue = Queue(-1)

            logger = m_transfer.get_logger("my_logger_name",
                                           queue=queue,
                                           log_level=log_level)
            self.assertIsInstance(logger, logging.Logger)
            self.assertEqual(logger.getEffectiveLevel(), logging_level)

        # --------------------------------------------------------------------
        # No logging module used
        # --------------------------------------------------------------------
        logger = m_transfer.get_logger("my_logger_name", queue=False)
        self.assertIsInstance(logger, hidra._shared_utils.LoggingFunction)

        # --------------------------------------------------------------------
        # Logging module used
        # --------------------------------------------------------------------
        test_logger_with_queue("debug", logging.DEBUG)
        test_logger_with_queue("info", logging.INFO)
        test_logger_with_queue("warning", logging.WARNING)
        test_logger_with_queue("error", logging.ERROR)
        test_logger_with_queue("critical", logging.CRITICAL)

    def test_generate_filepath(self):

        config_dict = {
            "relative_path": None,
            "filename": None
        }

        # --------------------------------------------------------------------
        # No base_path
        # --------------------------------------------------------------------
        ret_val = m_transfer.generate_filepath(base_path=None,
                                               config_dict=config_dict)
        self.assertIsNone(ret_val)

        # --------------------------------------------------------------------
        # No config_dict
        # --------------------------------------------------------------------
        ret_val = m_transfer.generate_filepath(base_path="test",
                                               config_dict=None)
        self.assertIsNone(ret_val)

        # --------------------------------------------------------------------
        # No relative_path (relative_path is None)
        # --------------------------------------------------------------------
        config_dict["relative_path"] = None
        ret_val = m_transfer.generate_filepath(base_path="test",
                                               config_dict=config_dict,
                                               add_filename=False)
        self.assertEqual(ret_val, "test")

        # --------------------------------------------------------------------
        # No relative_path (empty string)
        # --------------------------------------------------------------------
        config_dict["relative_path"] = ""
        ret_val = m_transfer.generate_filepath(base_path="test",
                                               config_dict=config_dict,
                                               add_filename=False)
        self.assertEqual(ret_val, "test")

        # --------------------------------------------------------------------
        # relative_path starts with slash
        # --------------------------------------------------------------------
        config_dict["relative_path"] = "/rel"
        ret_val = m_transfer.generate_filepath(base_path="test",
                                               config_dict=config_dict,
                                               add_filename=False)
        self.assertEqual(ret_val, "test/rel")

        # --------------------------------------------------------------------
        # relative_path ok
        # --------------------------------------------------------------------
        config_dict["relative_path"] = "rel"
        ret_val = m_transfer.generate_filepath(base_path="test",
                                               config_dict=config_dict,
                                               add_filename=False)
        self.assertEqual(ret_val, "test/rel")

        # --------------------------------------------------------------------
        # add filename
        # --------------------------------------------------------------------
        config_dict["relative_path"] = "rel"
        config_dict["filename"] = "my_file"
        ret_val = m_transfer.generate_filepath(base_path="test",
                                               config_dict=config_dict,
                                               add_filename=True)
        self.assertEqual(ret_val, "test/rel/my_file")

    def test_generate_file_identifier(self):
        config_dict = {
            "relative_path": None,
            "filename": None
        }

        # --------------------------------------------------------------------
        # no config_dict
        # --------------------------------------------------------------------
        ret_val = m_transfer.generate_file_identifier(config_dict=None)
        self.assertIsNone(ret_val)

        # --------------------------------------------------------------------
        # No relative_path (relative_path is None)
        # --------------------------------------------------------------------
        config_dict["relative_path"] = None
        config_dict["filename"] = "my_file"
        ret_val = m_transfer.generate_file_identifier(config_dict=config_dict)
        self.assertEqual(ret_val, "my_file")

        # --------------------------------------------------------------------
        # No relative_path (empty string)
        # --------------------------------------------------------------------
        config_dict["relative_path"] = ""
        config_dict["filename"] = "my_file"
        ret_val = m_transfer.generate_file_identifier(config_dict=config_dict)
        self.assertEqual(ret_val, "my_file")

        # --------------------------------------------------------------------
        # relative_path starts with slash
        # --------------------------------------------------------------------
        config_dict["relative_path"] = "/rel"
        config_dict["filename"] = "my_file"
        ret_val = m_transfer.generate_file_identifier(config_dict=config_dict)
        self.assertEqual(ret_val, "rel/my_file")

        # --------------------------------------------------------------------
        # relative_path ok
        # --------------------------------------------------------------------
        config_dict["relative_path"] = "rel"
        config_dict["filename"] = "my_file"
        ret_val = m_transfer.generate_file_identifier(config_dict=config_dict)
        self.assertEqual(ret_val, "rel/my_file")

    def test_convert_suffix_list_to_regex(self):

        # pylint: disable=invalid-name

        # --------------------------------------------------------------------
        # already regex (not compiled)
        # --------------------------------------------------------------------
        ret_val = m_transfer.convert_suffix_list_to_regex(pattern=".*",
                                                          suffix=True,
                                                          compile_regex=False)
        self.assertEqual(ret_val, ".*")

        # --------------------------------------------------------------------
        # already regex (compile)
        # --------------------------------------------------------------------
        ret_val = m_transfer.convert_suffix_list_to_regex(pattern=".*",
                                                          suffix=True,
                                                          compile_regex=True)
        self.assertEqual(ret_val, re.compile(".*"))

        # --------------------------------------------------------------------
        # all suffixes
        # --------------------------------------------------------------------
        ret_val = m_transfer.convert_suffix_list_to_regex(pattern=[""],
                                                          suffix=True,
                                                          compile_regex=False)
        self.assertEqual(ret_val, ".*")

        # --------------------------------------------------------------------
        # one file extention
        # --------------------------------------------------------------------
        ret_val = m_transfer.convert_suffix_list_to_regex(pattern=[".py"],
                                                          suffix=False,
                                                          compile_regex=False)
        self.assertEqual(ret_val, "(.py)$")

        # --------------------------------------------------------------------
        # multiple file extentions
        # --------------------------------------------------------------------
        ret_val = m_transfer.convert_suffix_list_to_regex(pattern=[".py", ".txt"],
                                                          suffix=False,
                                                          compile_regex=False)
        self.assertEqual(ret_val, "(.py|.txt)$")

        # --------------------------------------------------------------------
        # log enabled
        # --------------------------------------------------------------------
        log = mock.MagicMock()
        ret_val = m_transfer.convert_suffix_list_to_regex(pattern=".*",
                                                          log=log)

        self.assertGreater(len(log.method_calls), 0)

    def test__setup(self):
        current_func_name = inspect.currentframe().f_code.co_name

        conf = {
            "context": zmq.Context()
        }

        with mock.patch("hidra.transfer.Transfer._setup"):
            transfer = m_transfer.Transfer(**self.transfer_conf)

        def check_loggingfunction(log_level):
            self.transfer_conf["use_log"] = log_level
            with mock.patch("hidra.transfer.LoggingFunction") as mock_logfunc:
                transfer._setup(**self.transfer_conf)

                self.assertIsInstance(transfer.log, mock.MagicMock)
                self.assertTrue(mock_logfunc.called)
                mock_logfunc.called_with(use_log=log_level)

        # --------------------------------------------------------------------
        # external context
        # --------------------------------------------------------------------
        self.log.info("{}: EXTERNAL CONTEXT".format(current_func_name))

        self.transfer_conf["context"] = conf["context"]
        transfer._setup(**self.transfer_conf)

        self.assertTrue(transfer.ext_context)
        self.assertEqual(transfer.context, conf["context"])

        # cleanup
        # resetting QueueHandlers
        transfer.log.handlers = []

        # --------------------------------------------------------------------
        # no external context
        # --------------------------------------------------------------------
        self.log.info("{}: NO EXTERNAL CONTEXT".format(current_func_name))

        self.transfer_conf["context"] = None
        transfer._setup(**self.transfer_conf)

        self.assertFalse(transfer.ext_context)
        self.assertIsInstance(transfer.context, zmq.Context)

        # cleanup
        self.transfer_conf["context"] = conf["context"]
        transfer.context.destroy(0)
        transfer.context = None

        # --------------------------------------------------------------------
        # use LoggingFunction
        # --------------------------------------------------------------------
        self.log.info("{}: USE LOGGINGFUNCTION".format(current_func_name))

        check_loggingfunction("debug")
        check_loggingfunction("info")
        check_loggingfunction("warning")
        check_loggingfunction("error")
        check_loggingfunction("critical")

        # --------------------------------------------------------------------
        # use logging queue
        # --------------------------------------------------------------------
        self.log.info("{}: USE LOGGING QUEUE".format(current_func_name))

        self.transfer_conf["use_log"] = Queue(-1)
        with mock.patch("hidra.transfer.get_logger") as mock_get_logger:
            transfer._setup(**self.transfer_conf)

            self.assertTrue(mock_get_logger.called)

        # --------------------------------------------------------------------
        # use logging
        # --------------------------------------------------------------------
        self.log.info("{}: USE LOGGING".format(current_func_name))

        self.transfer_conf["use_log"] = True
        transfer._setup(**self.transfer_conf)

        self.assertIsInstance(transfer.log, logging.Logger)

        # --------------------------------------------------------------------
        # no logging
        # --------------------------------------------------------------------
        self.log.info("{}: NO LOGGING".format(current_func_name))

        check_loggingfunction(None)

        # --------------------------------------------------------------------
        # no logging method configured
        # --------------------------------------------------------------------
        self.log.info("{}: NO LOGGING METHOD CONFIGURED"
                      .format(current_func_name))

        check_loggingfunction("debug")

        # --------------------------------------------------------------------
        # not supported
        # --------------------------------------------------------------------
        self.log.info("{}: NOT SUPPORTED".format(current_func_name))

        self.transfer_conf["connection_type"] = None
        with self.assertRaises(m_transfer.NotSupported):
            transfer._setup(**self.transfer_conf)

    def test_get_remote_version(self):

        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # OK
        # --------------------------------------------------------------------
        m_create_socket = "hidra.transfer.Transfer._create_signal_socket"
        m_send_signal = "hidra.transfer.Transfer._send_signal"
        with mock.patch(m_create_socket) as mock_create_socket:
            with mock.patch(m_send_signal) as mock_send_signal:
                mock_send_signal.return_value = [b"GET_VERSION", "my_version"]

                ret_val = transfer.get_remote_version()

                self.assertTrue(mock_create_socket.called)
                self.assertTrue(mock_send_signal.called)
                self.assertEqual(ret_val, "my_version")

        # --------------------------------------------------------------------
        # Incorrect response
        # --------------------------------------------------------------------
        m_create_socket = "hidra.transfer.Transfer._create_signal_socket"
        m_send_signal = "hidra.transfer.Transfer._send_signal"
        with mock.patch(m_create_socket) as mock_create_socket:
            with mock.patch(m_send_signal) as mock_send_signal:
                mock_send_signal.return_value = [b"ERROR", "my_version"]

                ret_val = transfer.get_remote_version()

                self.assertTrue(mock_create_socket.called)
                self.assertTrue(mock_send_signal.called)
                self.assertEqual(ret_val, None)

    def test_set_appid(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)
        transfer.set_appid("my_appid")
        self.assertEqual(transfer.appid, "my_appid")

    def test_get_appid(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)
        transfer.appid = "my_appid"

        self.assertEqual(transfer.get_appid(), "my_appid")

    def test_initiate(self):
        # --------------------------------------------------------------------
        # NEXUS
        # --------------------------------------------------------------------
        transfer = m_transfer.Transfer(**self.transfer_conf)
        transfer.connection_type = "NEXUS"

        targets = []
        ret_val = transfer.initiate(targets)
        self.assertIsNone(ret_val)

        # --------------------------------------------------------------------
        # Wrong targets format
        # --------------------------------------------------------------------
        transfer = m_transfer.Transfer(**self.transfer_conf)

        wrong_targets = ""
        with self.assertRaises(m_transfer.FormatError):
            transfer.initiate(wrong_targets)

        # --------------------------------------------------------------------
        # successful
        # --------------------------------------------------------------------
        transfer = m_transfer.Transfer(**self.transfer_conf)

        targets = []

        m_create_socket = "hidra.transfer.Transfer._create_signal_socket"
        m_set_targets = "hidra.transfer.Transfer._set_targets"
        m_send_signal = "hidra.transfer.Transfer._send_signal"
        with mock.patch(m_create_socket) as mock_create_socket:
            with mock.patch(m_set_targets) as mock_set_targets:
                with mock.patch(m_send_signal) as mock_send_signal:
                    mock_send_signal.return_value = [
                        b"START_{}".format(self.transfer_conf["connection_type"])
                    ]

                    transfer.initiate(targets)

                    self.assertTrue(mock_create_socket.called)
                    self.assertTrue(mock_set_targets.called)
                    self.assertTrue(mock_send_signal.called)

                    # if no exception was raised test succeeded

        # --------------------------------------------------------------------
        # wrong response
        # --------------------------------------------------------------------
        transfer = m_transfer.Transfer(**self.transfer_conf)

        targets = []

        m_create_socket = "hidra.transfer.Transfer._create_signal_socket"
        m_set_targets = "hidra.transfer.Transfer._set_targets"
        m_send_signal = "hidra.transfer.Transfer._send_signal"
        with mock.patch(m_create_socket) as mock_create_socket:
            with mock.patch(m_set_targets) as mock_set_targets:
                with mock.patch(m_send_signal) as mock_send_signal:
                    mock_send_signal.return_value = [
                        "something_wrong"
                    ]

                    with self.assertRaises(m_transfer.CommunicationFailed):
                        transfer.initiate(targets)

    def test__create_signal_socket(self):
        # --------------------------------------------------------------------
        # no signal host
        # --------------------------------------------------------------------
        self.transfer_conf["signal_host"] = None
        transfer = m_transfer.Transfer(**self.transfer_conf)

        m_start_socket = "hidra.transfer.Transfer._start_socket"
        m_stop = "hidra.transfer.Transfer.stop"
        with mock.patch(m_start_socket) as mock_start_socket:
            with mock.patch(m_stop) as mock_stop:
                with self.assertRaises(m_transfer.ConnectionFailed):
                    transfer._create_signal_socket()

                self.assertTrue(mock_stop.called)

        # --------------------------------------------------------------------
        # OK
        # --------------------------------------------------------------------
        self.transfer_conf["signal_host"] = self.con_ip
        transfer = m_transfer.Transfer(**self.transfer_conf)

        m_start_socket = "hidra.transfer.Transfer._start_socket"
        m_stop = "hidra.transfer.Transfer.stop"
        with mock.patch(m_start_socket) as mock_start_socket:
            with mock.patch(m_stop) as mock_stop:
                transfer.poller = mock.MagicMock()

                transfer._create_signal_socket()

                self.assertTrue(mock_start_socket.called)
                transfer.poller.register.assert_called_once_with(
                    transfer.signal_socket,
                    zmq.POLLIN
                )

    def test__set_targets(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)

        host = self.con_ip
        host2 = "abc"
        port = 1234
        prio = 1
        suffixes = [""]

        # --------------------------------------------------------------------
        # target not a list
        # --------------------------------------------------------------------
        wrong_targets = ""

        with self.assertRaises(m_transfer.FormatError):
            transfer._set_targets(wrong_targets)

        # --------------------------------------------------------------------
        # one target without suffixes
        # --------------------------------------------------------------------
        targets = [host, port, prio]

        transfer._set_targets(targets)

        expected = [["{}:{}".format(host, port), prio, ".*"]]
        self.assertListEqual(transfer.targets, expected)

        # --------------------------------------------------------------------
        # one target with suffixes
        # --------------------------------------------------------------------
        targets = [host, port, prio, suffixes]

        m_convert = "hidra.transfer.convert_suffix_list_to_regex"
        with mock.patch(m_convert) as mock_convert:
            mock_convert.return_value = ".*"

            transfer._set_targets(targets)

        expected = [["{}:{}".format(host, port), prio, ".*"]]
        self.assertListEqual(transfer.targets, expected)

        # --------------------------------------------------------------------
        # multiple targets without suffixes
        # --------------------------------------------------------------------
        targets = [[host, port, prio], [host2, port, prio]]

        m_convert = "hidra.transfer.convert_suffix_list_to_regex"
        with mock.patch(m_convert) as mock_convert:
            mock_convert.return_value = ".*"

            transfer._set_targets(targets)

        expected = [
            ["{}:{}".format(host, port), prio, ".*"],
            ["{}:{}".format(host2, port), prio, ".*"]
        ]
        self.assertListEqual(transfer.targets, expected)

        # --------------------------------------------------------------------
        # multiple targets with suffixes
        # --------------------------------------------------------------------
        targets = [[host, port, prio, suffixes], [host2, port, prio, suffixes]]

        m_convert = "hidra.transfer.convert_suffix_list_to_regex"
        with mock.patch(m_convert) as mock_convert:
            mock_convert.return_value = ".*"

            transfer._set_targets(targets)

        expected = [
            ["{}:{}".format(host, port), prio, ".*"],
            ["{}:{}".format(host2, port), prio, ".*"]
        ]
        self.assertListEqual(transfer.targets, expected)

        # --------------------------------------------------------------------
        # multiple targets wrong format
        # --------------------------------------------------------------------
        targets = [[]]

        with self.assertRaises(m_transfer.FormatError):
            transfer._set_targets(targets)

    def test__send_signal(self):
        current_func_name = inspect.currentframe().f_code.co_name

        transfer = m_transfer.Transfer(**self.transfer_conf)

        def check_message(transfer, signal, exception):
            transfer.stop = MockZmqSocket()

            transfer.signal_socket = MockZmqSocket()
            transfer.signal_socket.recv_multipart.return_value = [signal, ""]

            transfer.poller = MockZmqPollerAllFake()
            transfer.poller.poll.return_value = {
                transfer.signal_socket: zmq.POLLIN
            }

            with self.assertRaises(exception):
                transfer._send_signal("foo")

            self.assertTrue(transfer.stop.called)

            # cleanup
            transfer.signal_socket = None
            transfer.poller = None
            transfer.stop = None

        # --------------------------------------------------------------------
        # no signal
        # --------------------------------------------------------------------
        self.log.info("{}: NO SIGNAL".format(current_func_name))

        ret_val = transfer._send_signal(None)

        self.assertIsNone(ret_val)

        # --------------------------------------------------------------------
        # Error when sending
        # --------------------------------------------------------------------
        self.log.info("{}: ERROR WHEN SENDING".format(current_func_name))

        transfer.signal_socket = MockZmqSocket()
        transfer.signal_socket.send_multipart.side_effect = TestException()

        with self.assertRaises(TestException):
            transfer._send_signal("foo")

        # cleanup
        transfer.signal_socket = None

        # --------------------------------------------------------------------
        # Error when polling
        # --------------------------------------------------------------------
        self.log.info("{}: ERROR WHEN POLLING".format(current_func_name))

        transfer.signal_socket = MockZmqSocket()
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.side_effect = TestException()

        with self.assertRaises(TestException):
            transfer._send_signal("foo")

        # cleanup
        transfer.signal_socket = None
        transfer.poller = None

        # --------------------------------------------------------------------
        # Error when receiving
        # --------------------------------------------------------------------
        self.log.info("{}: ERROR WHEN RECEIVING".format(current_func_name))

        transfer.signal_socket = MockZmqSocket()
        transfer.signal_socket.recv_multipart.side_effect = TestException()

        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {transfer.signal_socket: zmq.POLLIN}

        with self.assertRaises(TestException):
            transfer._send_signal("foo")

        # cleanup
        transfer.signal_socket = None
        transfer.poller = None

        # --------------------------------------------------------------------
        # Received VERSION_CONFLICT
        # --------------------------------------------------------------------
        self.log.info("{}: RECEIVED VERSION_CONFLICT"
                      .format(current_func_name))

        check_message(transfer,
                      b"VERSION_CONFLICT",
                      hidra.transfer.VersionError)

        # --------------------------------------------------------------------
        # Received NO_VALID_HOST
        # --------------------------------------------------------------------
        self.log.info("{}: RECEIVED NO_VALID_HOST".format(current_func_name))

        check_message(transfer,
                      b"NO_VALID_HOST",
                      hidra.transfer.AuthenticationFailed)

        # --------------------------------------------------------------------
        # Received CONNECTION_ALREADY_OPEN
        # --------------------------------------------------------------------
        self.log.info("{}: RECEIVED CONNECTION_ALREADY_OPEN"
                      .format(current_func_name))

        check_message(transfer,
                      b"CONNECTION_ALREADY_OPEN",
                      hidra.transfer.CommunicationFailed)

        # --------------------------------------------------------------------
        # Received STORING_DISABLED
        # --------------------------------------------------------------------
        self.log.info("{}: RECEIVED STORING_DISABLED"
                      .format(current_func_name))

        check_message(transfer,
                      b"STORING_DISABLED",
                      hidra.transfer.CommunicationFailed)

        # --------------------------------------------------------------------
        # Received NO_VALID_SIGNAL
        # --------------------------------------------------------------------
        self.log.info("{}: RECEIVED NO_VALID_SIGNAL"
                      .format(current_func_name))

        check_message(transfer,
                      b"NO_VALID_SIGNAL",
                      hidra.transfer.CommunicationFailed)

        # --------------------------------------------------------------------
        # Received not supported
        # --------------------------------------------------------------------
        self.log.info("{}: RECEIVED NOT SUPPORTED MESSAGE"
                      .format(current_func_name))

        transfer.stop = MockZmqSocket()

        transfer.signal_socket = MockZmqSocket()
        transfer.signal_socket.recv_multipart.return_value = [
            b"something not supported",
            ""
        ]

        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.signal_socket: zmq.POLLIN
        }

        ret_val = transfer._send_signal("foo")
        self.assertEqual(ret_val, ["something not supported", ""])

        # cleanup
        transfer.signal_socket = None
        transfer.poller = None
        transfer.stop = None

    @mock.patch("socket.getfqdn")
    def test__get_data_endpoint(self, mock_getfqdn):
        current_func_name = inspect.currentframe().f_code.co_name

        transfer = m_transfer.Transfer(**self.transfer_conf)
        transfer._updata_ip = mock.MagicMock()
        transfer._get_endpoint = mock.MagicMock(return_value="my_endpoint")

        host = self.con_ip
        port = 1234
        socket_id = "{}:{}".format(host, port).encode("utf-8")

        ipc_dir = "test_dir"
        ipc_file = "test_file"
        ipc_socket_id = "{}/{}".format(ipc_dir, ipc_file)

        mock_getfqdn.return_value = host

        # --------------------------------------------------------------------
        # data_socket_prop: list but wrong format
        # --------------------------------------------------------------------
        self.log.info("{}: DATA_SOCKET_PROP: LIST BUT WRONG FORMAT"
                      .format(current_func_name))

        data_socket_prop = []
        with self.assertRaises(hidra.transfer.FormatError):
            transfer._get_data_endpoint(data_socket_prop)

        # --------------------------------------------------------------------
        # data_socket_prop: list ok
        # --------------------------------------------------------------------
        self.log.info("{}: DATA_SOCKET_PROP: LIST OK"
                      .format(current_func_name))

        data_socket_prop = [host, port]
        ret_val = transfer._get_data_endpoint(data_socket_prop)

        self.assertEqual(ret_val, (socket_id, "my_endpoint"))

        # --------------------------------------------------------------------
        # data_socket_prop: port only
        # --------------------------------------------------------------------
        self.log.info("{}: DATA_SOCKET_PROP: PORT ONLY"
                      .format(current_func_name))

        data_socket_prop = port
        ret_val = transfer._get_data_endpoint(data_socket_prop)

        self.assertEqual(ret_val, (socket_id, "my_endpoint"))

        # --------------------------------------------------------------------
        # no data_socket_prop but correct targets
        # --------------------------------------------------------------------
        self.log.info("{}: NO DATA_SOCKET_PROP BUT CORRECT TARGETS"
                      .format(current_func_name))

        transfer.targets = [[socket_id, 1, ".*"]]
        ret_val = transfer._get_data_endpoint(data_socket_prop=None)

        self.assertEqual(ret_val, (socket_id, "my_endpoint"))

        # cleanup
        transfer.targets = None

        # --------------------------------------------------------------------
        # no data_socket_prop, too many targets
        # --------------------------------------------------------------------
        self.log.info("{}: NO DATA_SOCKET_PROP, TOO MANY TARGETS"
                      .format(current_func_name))

        transfer.targets = [[socket_id, 1, ".*"], [socket_id, 1, ".*"]]
        with self.assertRaises(hidra.transfer.FormatError):
            transfer._get_data_endpoint(data_socket_prop=None)

        # cleanup
        transfer.targets = None

        # --------------------------------------------------------------------
        # no data_socket_prop, no targets
        # --------------------------------------------------------------------
        self.log.info("{}: DATA_SOCKET_PROP, NO TARGETS"
                      .format(current_func_name))

        transfer.targets = None
        with self.assertRaises(hidra.transfer.FormatError):
            transfer._get_data_endpoint(data_socket_prop=None)

        # --------------------------------------------------------------------
        # zmq_protocol is ipc
        # --------------------------------------------------------------------
        self.log.info("{}: ZMQ_PROTOCOL IS IPC".format(current_func_name))

        transfer.zmq_protocol = "ipc"
        data_socket_prop = [ipc_dir, ipc_file]
        ret_val = transfer._get_data_endpoint(data_socket_prop)

        self.assertEqual(ret_val,
                         (ipc_socket_id, "ipc://{}".format(ipc_socket_id)))

        # cleanup
        transfer.zmq_protocol = None

        # --------------------------------------------------------------------
        # set IPs
        # --------------------------------------------------------------------
        self.log.info("{}: SET IPS".format(current_func_name))

        data_socket_prop = port
        with mock.patch("socket.gethostbyaddr") as mock_gethostbyaddr:
            mock_gethostbyaddr.return_value = ("", [""], ["my_ip"])
            ret_val = transfer._get_data_endpoint(data_socket_prop)

        self.assertEqual(transfer.ip, "my_ip")
        self.assertEqual(ret_val, (socket_id, "my_endpoint"))

        # --------------------------------------------------------------------
        # multiple possible IPs
        # --------------------------------------------------------------------
        self.log.info("{}: MULTIPLE POSSIBLE IPS".format(current_func_name))

        data_socket_prop = port
        with mock.patch("socket.gethostbyaddr") as mock_gethostbyaddr:
            mock_gethostbyaddr.return_value = ("", [""], ["my_ip", "second_ip"])

            with self.assertRaises(hidra.transfer.CommunicationFailed):
                transfer._get_data_endpoint(data_socket_prop)

        # --------------------------------------------------------------------
        # IPv4
        # --------------------------------------------------------------------
        self.log.info("{}: IPV4".format(current_func_name))

        data_socket_prop = [host, port]
        with mock.patch("socket.inet_aton") as mock_inet_aton:
            ret_val = transfer._get_data_endpoint(data_socket_prop)

        self.assertFalse(transfer.is_ipv6)
        self.assertEqual(ret_val, (socket_id, "my_endpoint"))

        # --------------------------------------------------------------------
        # IPv6
        # --------------------------------------------------------------------
        self.log.info("{}: IPV6".format(current_func_name))

        data_socket_prop = [host, port]
        with mock.patch("socket.inet_aton") as mock_inet_aton:
            mock_inet_aton.side_effect = socket.error
            ret_val = transfer._get_data_endpoint(data_socket_prop)

        self.assertTrue(transfer.is_ipv6)
        self.assertEqual(ret_val, (socket_id, "my_endpoint"))

    def test__update_ip(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)

        new_ip = "my_ip"

        transfer.ip = new_ip
        transfer._update_ip()

        self.assertEqual(transfer.status_check_conf["ip"], new_ip)
        self.assertEqual(transfer.file_op_conf["ip"], new_ip)
        self.assertEqual(transfer.confirmation_conf["ip"], new_ip)

    def test__get_endpoint(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)
        transfer._get_tcp_addr = mock.MagicMock(return_value="tcp_addr")
        transfer._get_ipc_addr = mock.MagicMock(return_value="ipc_addr")

        # --------------------------------------------------------------------
        # TCP
        # --------------------------------------------------------------------
        protocol = "tcp"
        ret_val = transfer._get_endpoint(protocol,
                                         ip=None,
                                         port=None,
                                         ipc_file=None)
        self.assertTrue(transfer._get_tcp_addr.called)
        self.assertEqual(ret_val, "tcp://tcp_addr")

        # --------------------------------------------------------------------
        # IPC
        # --------------------------------------------------------------------
        protocol = "ipc"
        ret_val = transfer._get_endpoint(protocol,
                                         ip=None,
                                         port=None,
                                         ipc_file=None)
        self.assertTrue(transfer._get_ipc_addr.called)
        self.assertEqual(ret_val, "ipc://ipc_addr")

    def test__get_tcp_addr(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)

        ip = "my_ip"
        port = 1234

        # --------------------------------------------------------------------
        # IPV4
        # --------------------------------------------------------------------
        transfer.is_ipv6 = False
        ret_val = transfer._get_tcp_addr(ip, port)
        self.assertEqual(ret_val, "{}:{}".format(ip, port))

        # --------------------------------------------------------------------
        # IPV4
        # --------------------------------------------------------------------
        transfer.is_ipv6 = True
        ret_val = transfer._get_tcp_addr(ip, port)
        self.assertEqual(ret_val, "[{}]:{}".format(ip, port))

    def test__get_ipc_addr(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)

        ipc_file = "test_ipc_file"
        ipc_dir = "test_ipc_dir"
        pid = 0000

        transfer.ipc_dir = ipc_dir
        transfer.current_pid = pid

        ret_val = transfer._get_ipc_addr(ipc_file)

        self.assertEqual(ret_val, "{}/{}_{}".format(ipc_dir, pid, ipc_file))

    def test_start(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)
        transfer.register = mock.MagicMock()
        transfer._get_data_endpoint = mock.MagicMock(
            return_value=("test_socket_id", "test_endpoint")
        )
        transfer._start_socket = mock.MagicMock()
        transfer.setopt = mock.MagicMock()
        transfer.poller = MockZmqPollerAllFake()

        # --------------------------------------------------------------------
        # Protocol not supported
        # --------------------------------------------------------------------

        with self.assertRaises(hidra.transfer.NotSupported):
            transfer.start(protocol="foo")

        # --------------------------------------------------------------------
        # data_con_style not supported
        # --------------------------------------------------------------------

        with self.assertRaises(hidra.transfer.NotSupported):
            transfer.start(data_con_style="foo")

        # --------------------------------------------------------------------
        # GENERAL
        # --------------------------------------------------------------------

        transfer.started_connections = {}
        transfer.connection_type = "QUERY_NEXT"
        transfer.request_socket = None

        transfer.start()

        self.assertTrue(transfer.register.called)
        self.assertEqual(transfer.data_socket_endpoint, "test_endpoint")
        self.assertTrue(transfer._get_data_endpoint.called)

        # cleanup
        transfer.request_socket = None

        # --------------------------------------------------------------------
        # QUERY_NEXT
        # --------------------------------------------------------------------

        transfer.started_connections = {}
        transfer.connection_type = "QUERY_NEXT"
        transfer.request_socket = None

        transfer.start()

        expected = {
            "QUERY_NEXT": {
                "id": "test_socket_id",
                "endpoint": "test_endpoint"
            }
        }
        self.assertEqual(transfer.started_connections, expected)
        self.assertTrue(transfer._start_socket.called)
        self.assertIsNotNone(transfer.request_socket)

        # cleanup
        transfer.request_socket = None

        # --------------------------------------------------------------------
        # NEXUS, ipc dir exists
        # --------------------------------------------------------------------

        transfer.started_connections = {}
        transfer.connection_type = "NEXUS"
        transfer.control_socket = None

        with mock.patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            with mock.patch("os.makedirs") as mock_makedirs:
                transfer.start()

                self.assertFalse(mock_makedirs.called)

        expected = {
            "NEXUS": {
                "id": "test_socket_id",
                "endpoint": "test_endpoint"
            }
        }
        self.assertEqual(transfer.started_connections, expected)
        self.assertTrue(transfer._start_socket.called)
        self.assertIsNotNone(transfer.control_socket)
        # pylint: disable=no-member
        self.assertTrue(transfer.poller.register.called)
        self.assertTrue(transfer.setopt.called)

        # cleanup
        transfer.control_socket = None

        # --------------------------------------------------------------------
        # NEXUS, ipc dir does not exist
        # --------------------------------------------------------------------

        transfer.started_connections = {}
        transfer.connection_type = "NEXUS"

        with mock.patch("os.path.exists") as mock_exists:
            mock_exists.return_value = False
            with mock.patch("os.makedirs") as mock_makedirs:
                transfer.start()

                self.assertTrue(mock_makedirs.called)

        # cleanup
        transfer.control_socket = None

        # --------------------------------------------------------------------
        # STREAM
        # --------------------------------------------------------------------

        transfer.started_connections = {}
        transfer.connection_type = "STREAM"

        transfer.start()

        expected = {
            "STREAM": {
                "id": "test_socket_id",
                "endpoint": "test_endpoint"
            }
        }
        self.assertEqual(transfer.started_connections, expected)

        # --------------------------------------------------------------------
        # reopen
        # --------------------------------------------------------------------

        transfer._get_data_endpoint.reset_mock()
        transfer.started_connections = {
            "STREAM": {
                "id": "test_socket_id",
                "endpoint": "test_endpoint"
            }
        }
        transfer.connection_type = "STREAM"

        transfer.start()

        expected = {
            "STREAM": {
                "id": "test_socket_id",
                "endpoint": "test_endpoint"
            }
        }
        self.assertEqual(transfer.started_connections, expected)
        self.assertFalse(transfer._get_data_endpoint.called)

    @mock.patch("hidra.transfer.Transfer.stop")
    def test_setopt(self, mock_stop):  # pylint: disable=unused-argument
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # status_check, already enabled
        # --------------------------------------------------------------------
        transfer.status_check_socket = "foo"
        transfer.log = mock.MagicMock()
        transfer.log.error = mock.MagicMock()

        option = "status_check"
        transfer.setopt(option)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("already enabled", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # status_check, ok
        # --------------------------------------------------------------------
        transfer._unpack_value = mock.MagicMock()
        transfer._get_endpoint = mock.MagicMock()
        transfer._start_socket = mock.MagicMock()
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.register = mock.MagicMock()
        transfer.status_check_socket = None

        option = "status_check"
        transfer.setopt(option)

        self.assertTrue(transfer._unpack_value.called)
        self.assertTrue(transfer._get_endpoint.called)
        self.assertTrue(transfer._start_socket.called)
        self.assertIsNotNone(transfer.status_check_socket)
        self.assertTrue(transfer.poller.register.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # file_op, already enabled
        # --------------------------------------------------------------------
        transfer.file_op_socket = "foo"
        transfer.log = mock.MagicMock()

        option = "file_op"
        transfer.setopt(option)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("already enabled", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # file_op, ok
        # --------------------------------------------------------------------
        transfer._unpack_value = mock.MagicMock()
        transfer._get_endpoint = mock.MagicMock()
        transfer._start_socket = mock.MagicMock()
        transfer.poller = MockZmqPollerAllFake()
        transfer.file_op_socket = None

        option = "file_op"
        transfer.setopt(option)

        self.assertTrue(transfer._unpack_value.called)
        self.assertTrue(transfer._get_endpoint.called)
        self.assertTrue(transfer._start_socket.called)
        self.assertIsNotNone(transfer.file_op_socket)
        self.assertTrue(transfer.poller.register.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # confirmation, already enabled
        # --------------------------------------------------------------------
        transfer.confirmation_socket = "foo"
        transfer.log = mock.MagicMock()

        option = "confirmation"
        transfer.setopt(option)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("already enabled", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # confirmation, ok
        # --------------------------------------------------------------------
        transfer._unpack_value = mock.MagicMock()
        transfer._get_endpoint = mock.MagicMock()
        transfer._start_socket = mock.MagicMock()
        transfer.confirmation_socket = None

        option = "confirmation"
        transfer.setopt(option)

        self.assertTrue(transfer._unpack_value.called)
        self.assertTrue(transfer._get_endpoint.called)
        self.assertTrue(transfer._start_socket.called)
        self.assertIsNotNone(transfer.confirmation_socket)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # Not supported
        # --------------------------------------------------------------------
        option = "foo"
        with self.assertRaises(hidra.transfer.NotSupported):
            transfer.setopt(option)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

    def test__unpack_value(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)

        protocol = "test_protocol"
        ip = "test_ip"
        port = 1234

        # --------------------------------------------------------------------
        # No value
        # --------------------------------------------------------------------

        value = None
        prop = {}

        transfer._unpack_value(value, prop)

        self.assertDictEqual(prop, {})

        # --------------------------------------------------------------------
        # value list, wrong format
        # --------------------------------------------------------------------

        value = []
        prop = {}

        with self.assertRaises(hidra.transfer.FormatError):
            transfer._unpack_value(value, prop)

        # --------------------------------------------------------------------
        # value list, no protocol
        # --------------------------------------------------------------------

        value = [ip, port]
        prop = {}

        transfer._unpack_value(value, prop)

        expected = {
            "ip": ip,
            "port": port
        }
        self.assertDictEqual(prop, expected)

        # --------------------------------------------------------------------
        # value list, with protocol
        # --------------------------------------------------------------------

        value = [protocol, ip, port]
        prop = {}

        transfer._unpack_value(value, prop)

        expected = {
            "protocol": protocol,
            "ip": ip,
            "port": port
        }
        self.assertDictEqual(prop, expected)

        # --------------------------------------------------------------------
        # port only
        # --------------------------------------------------------------------

        value = port
        prop = {}

        transfer._unpack_value(value, prop)

        expected = {
            "port": port
        }
        self.assertDictEqual(prop, expected)

    @mock.patch("hidra.transfer.Transfer.stop")
    @mock.patch("hidra.transfer.Transfer._start_socket")
    def test_register(self, mock_start_socket, mock_stop):
        # pylint: disable=unused-argument

        transfer = m_transfer.Transfer(**self.transfer_conf)
        transfer.poller = MockZmqPollerAllFake()
        transfer.data_socket = None

        # --------------------------------------------------------------------
        # no whitelist
        # --------------------------------------------------------------------
        mock_start_socket.reset_mock()

        whitelist = None
        transfer.register(whitelist)

        self.assertTrue(mock_start_socket.called)
        self.assertIsNotNone(transfer.data_socket)
        self.assertTrue(transfer.poller.register)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # whitelist: wrong format
        # --------------------------------------------------------------------

        wrong_whitelist = ""
        with self.assertRaises(hidra.transfer.FormatError):
            transfer.register(wrong_whitelist)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # whitelist ok but empty
        # --------------------------------------------------------------------

        whitelist = []

        with mock.patch("hidra.transfer.ThreadAuthenticator") as mock_auth:
            transfer.register(whitelist)

            self.assertTrue(mock_auth.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # whitelist: localhost
        # --------------------------------------------------------------------

        hidra.transfer.ThreadAuthenticator = MockZmqAuthenticator()

        host = "localhost"
        whitelist = [host]

        with mock.patch("socket.gethostbyname") as mock_gethostbyname:
            transfer.register(whitelist)

            self.assertTrue(mock_gethostbyname.called)
            self.assertTrue(transfer.auth.allow.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # whitelist: not localhost
        # --------------------------------------------------------------------

        hidra.transfer.ThreadAuthenticator = MockZmqAuthenticator()

        host = "test_host"
        whitelist = [host]

        with mock.patch("socket.gethostbyaddr") as mock_gethostbyaddr:
            mock_gethostbyaddr.return_value = "test_ip"

            transfer.register(whitelist)

            self.assertTrue(mock_gethostbyaddr.called)
            self.assertTrue(transfer.auth.allow.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # whitelist: getting ip fails
        # --------------------------------------------------------------------

        host = "test_host"
        whitelist = [host]

        with mock.patch("socket.gethostbyaddr") as mock_gethostbyaddr:
            mock_gethostbyaddr.side_effect = socket.gaierror()

            transfer.register(whitelist)

            self.assertTrue(mock_gethostbyaddr.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # whitelist: getting ip fails
        # --------------------------------------------------------------------
        transfer.log = mock.MagicMock()

        host = "test_host"
        whitelist = [host]

        with mock.patch("socket.gethostbyaddr") as mock_gethostbyaddr:
            mock_gethostbyaddr.side_effect = TestException()

            with self.assertRaises(m_transfer.AuthenticationFailed):
                transfer.register(whitelist)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

    def todo_test_read(self):
        pass

    def todo_test__react_on_message(self):
        pass

    @mock.patch("hidra.transfer.Transfer.stop")
    def test_get_chunk(self, mock_stop):
        # pylint: disable=unused-argument

        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # No connection open
        # --------------------------------------------------------------------
        transfer.started_connections = {}
        transfer.log = mock.MagicMock()

        ret_val = transfer.get_chunk()

        self.assertEqual(ret_val, (None, None))
        self.assertTrue(transfer.log.error.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # QUERY_NEXT, sending fails
        # --------------------------------------------------------------------

        transfer.log = mock.MagicMock()
        transfer.request_socket = MockZmqSocket()
        transfer.request_socket.send_multipart.side_effect = Exception()
        transfer.started_connections = {
            "QUERY_NEXT": {
                "id": None
            }
        }

        ret_val = transfer.get_chunk()

        self.assertEqual(ret_val, (None, None))
        self.assertTrue(transfer.log.error.called)
        self.assertIn("not send request", transfer.log.error.call_args[0][0])

        # cleanup
        transfer.request_socket = None
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # polling fails, stop active
        # --------------------------------------------------------------------

        transfer.log = mock.MagicMock()
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.side_effect = TestException()
        transfer.stopped_everything = True
        transfer.started_connections = {"STREAM": None}

        with self.assertRaises(KeyboardInterrupt):
            transfer.get_chunk()

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # polling fails, stop inactive
        # --------------------------------------------------------------------

        transfer.log = mock.MagicMock()
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.side_effect = TestException()
        transfer.stopped_everything = False
        transfer.started_connections = {"STREAM": None}

        with self.assertRaises(TestException):
            transfer.get_chunk()

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # status_check: STATUS_CHECK
        # --------------------------------------------------------------------

        transfer.started_connections = {"STREAM": None}
        transfer.status_check_socket = MockZmqSocket()
        transfer.status_check_socket.recv_multipart.return_value = [
            b"STATUS_CHECK"
        ]
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.status_check_socket: zmq.POLLIN
        }

        transfer.get_chunk()

        self.assertTrue(transfer.status_check_socket.send_multipart.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # status_check: RESET_STATUS
        # --------------------------------------------------------------------

        transfer.started_connections = {"STREAM": None}
        transfer.status_check_socket = MockZmqSocket()
        transfer.status_check_socket.recv_multipart.return_value = [
            b"RESET_STATUS"
        ]
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.status_check_socket: zmq.POLLIN
        }
        transfer.status = "foo"

        transfer.get_chunk()

        self.assertTrue(transfer.status_check_socket.send_multipart.called)
        self.assertEqual(transfer.status, [b"OK"])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # status_check: not supported
        # --------------------------------------------------------------------

        transfer.started_connections = {"STREAM": None}
        transfer.status_check_socket = MockZmqSocket()
        transfer.status_check_socket.recv_multipart.return_value = [
            b"foo"
        ]
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.status_check_socket: zmq.POLLIN
        }

        transfer.get_chunk()

        transfer.status_check_socket.send_multipart.called_once_with([b"ERROR"])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # data: receiving fails
        # --------------------------------------------------------------------

        transfer.log = mock.MagicMock()
        transfer.started_connections = {"STREAM": None}
        transfer.data_socket = MockZmqSocket()
        transfer.data_socket.recv_multipart.side_effect = Exception()
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.data_socket: zmq.POLLIN
        }

        ret_val = transfer.get_chunk()

        self.assertTrue(transfer.log.error.called)
        self.assertIn("failed", transfer.log.error.call_args[0][0])
        self.assertEqual(ret_val, [None, None])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # data: ALIVE_TEST, no timeout
        # --------------------------------------------------------------------

        transfer.started_connections = {"STREAM": None}
        transfer.data_socket = MockZmqSocket()
        transfer.data_socket.recv_multipart.return_value = [
            b"ALIVE_TEST"
        ]
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.data_socket: zmq.POLLIN
        }

        timeout = -1
        ret_val = transfer.get_chunk(timeout)

        self.assertEqual(ret_val, [None, None])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # data: ALIVE_TEST, timeout
        # --------------------------------------------------------------------

        transfer.started_connections = {"STREAM": None}
        transfer.data_socket = MockZmqSocket()
        transfer.data_socket.recv_multipart.return_value = [
            b"ALIVE_TEST"
        ]
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.data_socket: zmq.POLLIN
        }

        timeout = 1
        with mock.patch("time.time") as mock_time:
            # The side effect values have to be different from each other,
            # otherwise the difference becomes 0
            mock_time.side_effect = [1, 2]
            ret_val = transfer.get_chunk(timeout)

            self.assertEqual(mock_time.call_count, 2)

        self.assertEqual(ret_val, [None, None])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # data: too short message
        # --------------------------------------------------------------------

        transfer.log = mock.MagicMock()
        transfer.started_connections = {"STREAM": None}
        transfer.data_socket = MockZmqSocket()
        transfer.data_socket.recv_multipart.return_value = ["foo"]
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.data_socket: zmq.POLLIN
        }

        ret_val = transfer.get_chunk()

        self.assertEqual(ret_val, [None, None])
        self.assertTrue(transfer.log.error.called)
        self.assertIn("too short", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # data: message ok
        # --------------------------------------------------------------------

        transfer.started_connections = {"STREAM": None}
        transfer.data_socket = MockZmqSocket()
        metadata = {"foo": None}
        transfer.data_socket.recv_multipart.return_value = [
            json.dumps(metadata).encode("utf-8"),
            "bar"
        ]
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.data_socket: zmq.POLLIN
        }

        ret_val = transfer.get_chunk()

        self.assertEqual(ret_val, [metadata, "bar"])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # data: metadata error
        # --------------------------------------------------------------------

        transfer.log = mock.MagicMock()
        transfer.started_connections = {"STREAM": None}
        transfer.data_socket = MockZmqSocket()
        transfer.data_socket.recv_multipart.return_value = [
            "wrong metadata",
            "bar"
        ]
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {
            transfer.data_socket: zmq.POLLIN
        }

        ret_val = transfer.get_chunk()

        self.assertTrue(transfer.log.error.called)
        self.assertIn("extract metadata", transfer.log.error.call_args[0][0])

        self.assertEqual(ret_val, [None, "bar"])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # run in timeout
        # --------------------------------------------------------------------

        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {}
        transfer.started_connections = {"STREAM": None}
        transfer.request_socket = MockZmqSocket()

        ret_val = transfer.get_chunk()

        self.assertEqual(ret_val, [None, None])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # run in timeout, QUERY_NEXT
        # --------------------------------------------------------------------

        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {}
        transfer.request_socket = MockZmqSocket()
        transfer.started_connections = {
            "QUERY_NEXT": {
                "id": None
            }
        }

        ret_val = transfer.get_chunk()

        expected = [
            mock.call([b"NEXT", None]),
            mock.call([b"CANCEL", None])
        ]
        self.assertEqual(transfer.request_socket.send_multipart.call_args_list,
                         expected)

        self.assertEqual(ret_val, [None, None])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # run in timeout, QUERY_NEXT, cancel fails
        # --------------------------------------------------------------------

        transfer.log = mock.MagicMock()
        transfer.poller = MockZmqPollerAllFake()
        transfer.poller.poll.return_value = {}
        transfer.request_socket = MockZmqSocket()
        transfer.request_socket.send_multipart.side_effect = [None, TestException()]
        transfer.started_connections = {
            "QUERY_NEXT": {
                "id": None
            }
        }

        ret_val = transfer.get_chunk()

        self.assertEqual(ret_val, [None, None])
        self.assertTrue(transfer.log.error.called)
        self.assertIn("not cancel", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

    def test_check_file_closed(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)

        payload = "1"

        # --------------------------------------------------------------------
        # payload size is smaller than chunksize
        # --------------------------------------------------------------------

        metadata = {
            "chunksize": 2,
            "filesize": 3,
            "chunk_number": 1
        }

        ret_val = transfer.check_file_closed(metadata, payload)
        self.assertTrue(ret_val)

        # --------------------------------------------------------------------
        # original size is multiple of chunksize
        # --------------------------------------------------------------------

        # chunk_number starts with 0

        metadata = {
            "chunksize": 1,
            "filesize": 2,
            "chunk_number": 1
        }

        ret_val = transfer.check_file_closed(metadata, payload)
        self.assertTrue(ret_val)

        # --------------------------------------------------------------------
        # not the last chunk
        # --------------------------------------------------------------------

        metadata = {
            "chunksize": 1,
            "filesize": 2,
            "chunk_number": 0
        }

        ret_val = transfer.check_file_closed(metadata, payload)
        self.assertFalse(ret_val)

    @mock.patch("hidra.transfer.Transfer.get_chunk")
    @mock.patch("hidra.transfer.Transfer.check_file_closed")
    def test_get(self, mock_check_file_closed, mock_get_chunk):
        current_func_name = inspect.currentframe().f_code.co_name

        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # all data in one iteration
        # --------------------------------------------------------------------
        self.log.info("{}: ALL DATA IN ONE ITERATION"
                      .format(current_func_name))

        metadata = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now",
            "chunk_number": 0
        }
        mock_get_chunk.side_effect = [[metadata, ""]]
        mock_check_file_closed.side_effect = [True]

        ret_metadata, ret_data = transfer.get()

        expected = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now",
            "chunk_number": None
        }
        self.assertDictEqual(ret_metadata, expected)
        self.assertEqual(ret_data, "")

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()

        # --------------------------------------------------------------------
        # all data in two iteration
        # --------------------------------------------------------------------
        self.log.info("{}: ALL DATA IN TWO ITERATION"
                      .format(current_func_name))

        metadata0 = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now0",
            "chunk_number": 0
        }
        metadata1 = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now1",
            "chunk_number": 1
        }
        mock_get_chunk.side_effect = [[metadata0, "part0"],
                                      [metadata1, "part1"]]
        mock_check_file_closed.side_effect = [False, True]

        ret_metadata, ret_data = transfer.get()

        expected = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now0",
            "chunk_number": None
        }
        self.assertDictEqual(ret_metadata, expected)
        self.assertEqual(ret_data, "part0part1")

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()

        # --------------------------------------------------------------------
        # KeyboardInterrupt
        # --------------------------------------------------------------------
        self.log.info("{}: KEYBOARDINTERRUPT".format(current_func_name))

        mock_check_file_closed.reset_mock()
        metadata = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now",
            "chunk_number": 0
        }
        mock_get_chunk.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            transfer.get()

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()

        # --------------------------------------------------------------------
        # Exception when getting chunk, not stopped
        # --------------------------------------------------------------------
        self.log.info("{}: EXCEPTION WHEN GETTING CHUNK, NOT STOPPED"
                      .format(current_func_name))

        transfer.log = mock.MagicMock()
        metadata = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now",
            "chunk_number": 0
        }
        mock_get_chunk.side_effect = TestException()
        transfer.stopped_everything = False

        with self.assertRaises(TestException):
            transfer.get()

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()

        # --------------------------------------------------------------------
        # Exception when getting chunk, stopped
        # --------------------------------------------------------------------
        self.log.info("{}: EXCEPTION WHEN GETTING CHUNK, STOPPED"
                      .format(current_func_name))

        mock_check_file_closed.reset_mock()
        metadata = {
            "file_mod_time": "now",
            "chunk_number": 0
        }
        mock_get_chunk.side_effect = TestException()
        transfer.stopped_everything = True

        transfer.get()
        self.assertFalse(mock_check_file_closed.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()

        # --------------------------------------------------------------------
        # Exception when joining
        # --------------------------------------------------------------------
        self.log.info("{}: EXCEPTION WHEN JOINING"
                      .format(current_func_name))

        transfer.log = mock.MagicMock()
        mock_check_file_closed.reset_mock()
        metadata = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now",
            "chunk_number": 0
        }
        # data is set to int to trigger a TypeError
        # (string is required to pass)
        mock_get_chunk.side_effect = [[metadata, 1]]
        mock_check_file_closed.side_effect = [True]

        with self.assertRaises(TypeError):
            transfer.get()

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()

        # --------------------------------------------------------------------
        # chunks of multiple files received intermixed
        # --------------------------------------------------------------------
        self.log.info("{}: CHUNKS OF MULITPLE FILES RECEIVED INTERMIXED"
                      .format(current_func_name))

        metadata0 = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now0",
            "chunk_number": 0
        }
        metadata1 = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now1",
            "chunk_number": 1
        }

        metadata_extra_chunk = {
            "relative_path": "test_rel_path",
            "filename": "test_filename2",
            "file_mod_time": "now2",
            "chunk_number": 0
        }
        mock_get_chunk.side_effect = [[metadata0, "part0"],
                                      [metadata_extra_chunk, "extra_part0"],
                                      [metadata1, "part1"]]
        mock_check_file_closed.side_effect = [False, False, True]

        ret_metadata, ret_data = transfer.get()

        expected = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now0",
            "chunk_number": None
        }
        self.assertDictEqual(ret_metadata, expected)
        self.assertEqual(ret_data, "part0part1")

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()

        # --------------------------------------------------------------------
        # reach timeout
        # --------------------------------------------------------------------
        self.log.info("{}: REACH TIMEOUT".format(current_func_name))

        metadata = {
            "relative_path": "test_rel_path",
            "filename": "test_filename",
            "file_mod_time": "now",
            "chunk_number": 0
        }
        mock_get_chunk.side_effect = [[None, None]]

        ret_metadata, ret_data = transfer.get(timeout=100)

        self.assertIsNone(ret_metadata)
        self.assertIsNone(ret_data)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_check_file_closed.reset_mock()


    @mock.patch("hidra.transfer.generate_file_identifier")
    @mock.patch("hidra.transfer.generate_filepath")
    @mock.patch("hidra.transfer.Transfer.check_file_closed")
    def test_store_chunk(self,
                         mock_check_file_closed,
                         mock_generate_filepath,
                         mock_gen_file_id):
        current_func_name = inspect.currentframe().f_code.co_name

        transfer = m_transfer.Transfer(**self.transfer_conf)

        mock_file = mock.MagicMock(write=mock.MagicMock(),
                                   close=mock.MagicMock())
        mock_gen_file_id.return_value = "test_file_id"

        filepath = "test_filepath"
        payload = ""
        base_path = "test_base_path"

        class TestIOError(IOError):
            def __init__(self, **kwargs):
                super(TestIOError, self).__init__(**kwargs)
                self.errno = errno.ENOENT

        # multiple calls of open (one of which is returns an exception)
        vars_calls = ["raise", ""]
        def mock_two_calls(*args):  # pylint: disable=unused-argument
            # for some reason there are more than just two calls
            # open transfer.py is also mocked (why?)
            try:
                call = vars_calls.pop(0)
            except Exception:
                call = ""

            if call == "raise":
                raise TestIOError
            else:
                return mock.MagicMock()


        # --------------------------------------------------------------------
        # open descriptors, file not closed
        # --------------------------------------------------------------------
        self.log.info("{}: OPEN DESCRIPTORS, FILE NOT CLOSED"
                      .format(current_func_name))

        descriptors = {
            "test_filepath": {
                "last_chunk_number": 0,
                "file": mock_file
            }
        }
        metadata = {
            "chunk_number": 1
        }

        mock_check_file_closed.side_effect = [False]

        with mock.patch("__builtin__.open") as mock_open:
            ret_val = transfer.store_chunk(descriptors,
                                           filepath,
                                           payload,
                                           base_path,
                                           metadata)

        self.assertTrue(ret_val)
        mock_file.write.assert_called_once_with(payload)

        # cleanup
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()
        mock_file.reset_mock()

        # --------------------------------------------------------------------
        # No open descriptors
        # --------------------------------------------------------------------
        self.log.info("{}: NO OPEN DESCRIPTORS".format(current_func_name))

        descriptors = {}
        metadata = {
            "chunk_number": 0
        }

        mock_check_file_closed.side_effect = [False]

        with mock.patch("__builtin__.open") as mock_open:
            ret_val = transfer.store_chunk(descriptors,
                                           filepath,
                                           payload,
                                           base_path,
                                           metadata)

        self.assertTrue(ret_val)

        # dictEqual does not work because "file" is a different instance
        # expected = {
        #     "test_filepath": {
        #         "last_chunk_number": 0,
        #         "file": MagicMock
        #     }
        # }
        self.assertIsInstance(descriptors, dict)
        self.assertIn("test_filepath", descriptors)
        self.assertIn("last_chunk_number", descriptors["test_filepath"])
        self.assertEqual(descriptors["test_filepath"]["last_chunk_number"], 0)
        self.assertIn("file", descriptors["test_filepath"])
        self.assertIsInstance(descriptors["test_filepath"]["file"], mock.MagicMock)

        # cleanup
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()

        # --------------------------------------------------------------------
        # No open descriptors, "No such file or directory" but exception
        # --------------------------------------------------------------------
        self.log.info("{}: NO OPEN DESCRIPTORS, 'NO SUCH FILE OR DIRECTORY' "
                      "but exception".format(current_func_name))

        descriptors = {}
        metadata = {
            # a missing "relative_path" triggers a KeyError
            "chunk_number": 0
        }

        transfer.log = mock.MagicMock()

        with mock.patch("__builtin__.open") as mock_open:
            # errno.ENOENT == "No such file or directory"
            mock_open.side_effect = TestIOError()

            with self.assertRaises(KeyError):
                transfer.store_chunk(descriptors,
                                     filepath,
                                     payload,
                                     base_path,
                                     metadata)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("open file", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # No open descriptors, "No such file or directory", all dirs allowed
        # --------------------------------------------------------------------
        self.log.info("{}: NO OPEN DESCRIPTORS, 'NO SUCH FILE OR DIRECTORY', "
                      "ALL DIRS ALLOWED".format(current_func_name))

        descriptors = {}
        metadata = {
            "relative_path": "test_rel_path",
            "chunk_number": 0
        }

        mock_generate_filepath.reset_mock()
        mock_check_file_closed.side_effect = [False]

        # multiple calls of open (one of which is returns an exception)
        vars_calls = ["raise", ""]

        with mock.patch("os.makedirs") as mock_makedirs:
            with mock.patch("__builtin__.open") as mock_open:
                # errno.ENOENT == "No such file or directory"
                mock_open.side_effect = mock_two_calls

                transfer.store_chunk(descriptors,
                                     filepath,
                                     payload,
                                     base_path,
                                     metadata)

                self.assertTrue(mock_makedirs.called)

        self.assertTrue(mock_generate_filepath.called)
        self.assertTrue(ret_val)

        # dictEqual does not work because "file" is a different instance
        # expected = {
        #     "test_filepath": {
        #         "last_chunk_number": 0,
        #         "file": MagicMock
        #     }
        # }
        self.assertIsInstance(descriptors, dict)
        self.assertIn("test_filepath", descriptors)
        self.assertIn("last_chunk_number", descriptors["test_filepath"])
        self.assertEqual(descriptors["test_filepath"]["last_chunk_number"], 0)
        self.assertIn("file", descriptors["test_filepath"])
        self.assertIsInstance(descriptors["test_filepath"]["file"], mock.MagicMock)

        # cleanup
        mock_generate_filepath.reset_mock()

        # --------------------------------------------------------------------
        # No open descriptors, "No such file or directory", No dirs allowed
        # --------------------------------------------------------------------
        self.log.info("{}: NO OPEN DESCRIPTORS, 'NO SUCH FILE OR DIRECTORY', "
                      "NO DIRS ALLOWED".format(current_func_name))

        descriptors = {}
        metadata = {
            "relative_path": "test_rel_path",
            "chunk_number": 0
        }

        mock_generate_filepath.reset_mock()
        mock_check_file_closed.side_effect = [False]

        # multiple calls of open (one of which is returns an exception)
        vars_calls = ["raise", ""]

        transfer.log = mock.MagicMock()
        transfer.dirs_not_to_create = "test_rel_path"

        with mock.patch("os.makedirs") as mock_makedirs:
            with mock.patch("__builtin__.open") as mock_open:
                # errno.ENOENT == "No such file or directory"
                mock_open.side_effect = mock_two_calls

                with self.assertRaises(TestIOError):
                    transfer.store_chunk(descriptors,
                                         filepath,
                                         payload,
                                         base_path,
                                         metadata)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("write file", transfer.log.error.call_args_list[0][0][0])
        self.assertFalse(mock_generate_filepath.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_generate_filepath.reset_mock()
        mock_check_file_closed.side_effect = None

        # --------------------------------------------------------------------
        # No open descriptors, not "no such file or directory"
        # --------------------------------------------------------------------
        self.log.info("{}: NO OPEN DESCRIPTORS, NOT 'NO SUCH FILE OR "
                      "DIRECTORY'".format(current_func_name))

        descriptors = {}
        metadata = {
            "chunk_number": 0
        }

        transfer.log = mock.MagicMock()

        with mock.patch("__builtin__.open") as mock_open:
            mock_open.side_effect = IOError()

            with self.assertRaises(IOError):
                transfer.store_chunk(descriptors,
                                     filepath,
                                     payload,
                                     base_path,
                                     metadata)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("append payload", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # No open descriptors, unknown exception when open
        # --------------------------------------------------------------------
        self.log.info("{}: NO OPEN DESCRIPTORS, UNKNOWN EXCEPTION WHEN OPEN"
                      .format(current_func_name))

        descriptors = {}
        metadata = {
            "chunk_number": 0
        }

        transfer.log = mock.MagicMock()

        with mock.patch("__builtin__.open") as mock_open:
            mock_open.side_effect = TestException()

            with self.assertRaises(TestException):
                transfer.store_chunk(descriptors,
                                     filepath,
                                     payload,
                                     base_path,
                                     metadata)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("append payload", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # KeyboardInterrupt in write
        # --------------------------------------------------------------------
        self.log.info("{}: KEYBOARDINTERRUPT IN WRITE"
                      .format(current_func_name))

        mock_file.write.side_effect = KeyboardInterrupt()
        descriptors = {
            "test_filepath": {
                "last_chunk_number": 0,
                "file": mock_file
            }
        }
        metadata = {
            "chunk_number": 1
        }

        with self.assertRaises(KeyboardInterrupt):
            transfer.store_chunk(descriptors,
                                 filepath,
                                 payload,
                                 base_path,
                                 metadata)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_file = mock.MagicMock(write=mock.MagicMock())

        # --------------------------------------------------------------------
        # descriptors of wrong format
        # --------------------------------------------------------------------
        self.log.info("{}: DESCRIPTORS OF WRONG FORMAT"
                      .format(current_func_name))

        mock_file.write.side_effect = TestException()
        descriptors = {
            "test_filepath": {
                "last_chunk_number": 0,
                "file": mock_file
            }
        }
        metadata = {
            "chunk_number": 1
        }

        transfer.log = mock.MagicMock()

        with self.assertRaises(TestException):
            transfer.store_chunk(descriptors,
                                 filepath,
                                 payload,
                                 base_path,
                                 metadata)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("append payload", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_file = mock.MagicMock(write=mock.MagicMock())

        # --------------------------------------------------------------------
        # chunk_number not in order
        # --------------------------------------------------------------------
        self.log.info("{}: CHUNK_NUMBER NOT IN ORDER"
                      .format(current_func_name))

        descriptors = {
            "test_filepath": {
                "last_chunk_number": 1,
                "file": mock_file
            }
        }
        metadata = {
            "chunk_number": 0
        }

        mock_check_file_closed.side_effect = [False]

        with mock.patch("__builtin__.open") as mock_open:
            transfer.store_chunk(descriptors,
                                 filepath,
                                 payload,
                                 base_path,
                                 metadata)

        # dictEqual does not work because "file" is a different instance
        # expected = {
        #     "test_filepath": {
        #         "last_chunk_number": 0,
        #         "file": MagicMock
        #     }
        # }
        self.assertIsInstance(descriptors, dict)
        self.assertIn("test_filepath", descriptors)
        self.assertIn("last_chunk_number", descriptors["test_filepath"])
        self.assertEqual(descriptors["test_filepath"]["last_chunk_number"], 0)
        self.assertIn("file", descriptors["test_filepath"])
        self.assertIsInstance(descriptors["test_filepath"]["file"], mock.MagicMock)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_file = mock.MagicMock(write=mock.MagicMock())


        # --------------------------------------------------------------------
        # send confirmation: not enabled
        # --------------------------------------------------------------------
        self.log.info("{}: SEND CONFIRMATION: NOT ENABLED"
                      .format(current_func_name))

        descriptors = {
            "test_filepath": {
                "last_chunk_number": 0,
                "file": mock_file
            }
        }
        metadata = {
            "chunk_number": 1,
            "confirmation_required": True
        }

        mock_check_file_closed.side_effect = [False]
        transfer.confirmation_socket = None

        with self.assertRaises(m_transfer.UsageError):
            transfer.store_chunk(descriptors,
                                 filepath,
                                 payload,
                                 base_path,
                                 metadata)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()
        mock_file.reset_mock()

        # --------------------------------------------------------------------
        # send confirmation: OK (backwards compatible)
        # --------------------------------------------------------------------
        self.log.info("{}: SEND CONFIRMATION: OK (BACKWARDS COMPATIBLE)"
                      .format(current_func_name))

        descriptors = {
            "test_filepath": {
                "last_chunk_number": 0,
                "file": mock_file
            }
        }
        metadata = {
            "chunk_number": 1,
            "confirmation_required": "test_topic"
        }

        mock_check_file_closed.side_effect = [False]
        transfer.confirmation_socket = mock.MagicMock()
        transfer.confirmation_socket.send_multipart = mock.MagicMock()

#        transfer.confirmation_socket = mock.MagicMock(
#            send_multipart=mock.MagicMock()
#        )

        # would have been set in initiate
        transfer._remote_version = b"4.0.7"

        transfer.store_chunk(descriptors,
                             filepath,
                             payload,
                             base_path,
                             metadata)

        self.assertTrue(transfer.confirmation_socket.send_multipart.called)
        transfer.confirmation_socket.send_multipart.assert_called_once_with(
            ["test_topic", "test_file_id"]
        )

        # cleanup
        transfer.confirmation_socket = None
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()
        mock_file.reset_mock()

        # --------------------------------------------------------------------
        # send confirmation: OK
        # --------------------------------------------------------------------
        self.log.info("{}: SEND CONFIRMATION: OK".format(current_func_name))

        descriptors = {
            "test_filepath": {
                "last_chunk_number": 0,
                "file": mock_file
            }
        }
        metadata = {
            "chunk_number": 1,
            "confirmation_required": "test_topic",
            "version": __version__
        }

        mock_check_file_closed.side_effect = [False]
        transfer.confirmation_socket = mock.MagicMock(
            send_multipart=mock.MagicMock()
        )

        # would have been set in initiate
        transfer._remote_version = __version__

        transfer.store_chunk(descriptors,
                             filepath,
                             payload,
                             base_path,
                             metadata)

        self.assertTrue(transfer.confirmation_socket.send_multipart.called)
        transfer.confirmation_socket.send_multipart.assert_called_once_with(
            ["test_topic", "test_file_id", "1"]
        )

        # cleanup
        transfer.confirmation_socket = None
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()
        mock_file.reset_mock()


        # --------------------------------------------------------------------
        # send confirmation: error
        # --------------------------------------------------------------------
        self.log.info("{}: SEND CONFIRMATION: ERROR"
                      .format(current_func_name))

        descriptors = {
            "test_filepath": {
                "last_chunk_number": 0,
                "file": mock_file
            }
        }
        metadata = {
            "chunk_number": 1,
            "confirmation_required": "test_topic",
            "version": __version__
        }

        mock_check_file_closed.side_effect = [False]
        transfer.confirmation_socket = mock.MagicMock(
            send_multipart=mock.MagicMock()
        )
        transfer.confirmation_socket.send_multipart.side_effect = TestException()
        # would have been set in initiate
        transfer._remote_version = __version__

        with self.assertRaises(TestException):
            transfer.store_chunk(descriptors,
                                 filepath,
                                 payload,
                                 base_path,
                                 metadata)

        self.assertTrue(transfer.confirmation_socket.send_multipart.called)
        transfer.confirmation_socket.send_multipart.assert_called_once_with(
            ["test_topic", "test_file_id", "1"]
        )

        # cleanup
        transfer.confirmation_socket = None
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()
        mock_file.reset_mock()

        # --------------------------------------------------------------------
        # open descriptors, file closed
        # --------------------------------------------------------------------
        self.log.info("{}: OPEN DESCRIPTORS, FILE CLOSED"
                      .format(current_func_name))

        descriptors = {
            "test_filepath": {
                "last_chunk_number": 0,
                "file": mock_file
            }
        }
        metadata = {
            "chunk_number": 1,
            "file_mod_time": "now"
        }

        mock_check_file_closed.side_effect = [True]

        ret_val = transfer.store_chunk(descriptors,
                                       filepath,
                                       payload,
                                       base_path,
                                       metadata)

        self.assertFalse(ret_val)
        mock_file.close.assert_called_once_with()

        self.assertDictEqual(descriptors, {})

        # cleanup
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()
        mock_file.reset_mock()

        # --------------------------------------------------------------------
        # open descriptors, file closed but error
        # --------------------------------------------------------------------
        self.log.info("{}: OPEN DESCRIPTORS, FILE CLOSED BUT ERROR"
                      .format(current_func_name))

        mock_file_error = mock.MagicMock(close=mock.MagicMock())
        mock_file_error.close.side_effect = TestException()

        descriptors = {
            "test_filepath": {
                "last_chunk_number": 0,
                "file": mock_file_error
            }
        }
        metadata = {
            "chunk_number": 1,
            "file_mod_time": "now"
        }

        transfer.log = mock.MagicMock()
        mock_check_file_closed.side_effect = [True]

        with self.assertRaises(TestException):
            transfer.store_chunk(descriptors,
                                 filepath,
                                 payload,
                                 base_path,
                                 metadata)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("could not be closed",
                      transfer.log.error.call_args[0][0])

        # cleanup
        mock_check_file_closed.side_effect = None
        mock_check_file_closed.reset_mock()

    @mock.patch("hidra.transfer.generate_filepath")
    @mock.patch("hidra.transfer.Transfer.store_chunk")
    @mock.patch("hidra.transfer.Transfer.get_chunk")
    def test_store(self,
                   mock_get_chunk,
                   mock_store_chunk,
                   mock_gen_filepath):

        current_func_name = inspect.currentframe().f_code.co_name

        transfer = m_transfer.Transfer(**self.transfer_conf)

        target_base_path = "test_base_path"

        # --------------------------------------------------------------------
        # one iteration, nothing received
        # --------------------------------------------------------------------
        self.log.info("{}: ONE ITERATION, NOTHING RECEIVED"
                      .format(current_func_name))

        metadata = None
        payload = None

        mock_get_chunk.side_effect = [[metadata, payload]]

        transfer.store(target_base_path)

        self.assertTrue(mock_get_chunk.called)
        self.assertFalse(mock_gen_filepath.called)

        # cleanup
        mock_get_chunk.reset_mock()

        # --------------------------------------------------------------------
        # one iteration, KeyboardInterrupt during receive
        # --------------------------------------------------------------------
        self.log.info("{}: ONE ITERATION, KEYBOARDINTERRUPT DURING RECEIVE"
                      .format(current_func_name))

        metadata = None
        payload = None

        mock_get_chunk.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            transfer.store(target_base_path)

        # cleanup
        mock_get_chunk.reset_mock()

        # --------------------------------------------------------------------
        # one iteration, error during receive, not stopped
        # --------------------------------------------------------------------
        self.log.info("{}: ONE ITERATION, ERROR DURING RECEIVE, NOT STOPPED"
                      .format(current_func_name))

        metadata = None
        payload = None

        mock_get_chunk.side_effect = TestException()
        transfer.stopped_everything = False
        transfer.log = mock.MagicMock()

        with self.assertRaises(TestException):
            transfer.store(target_base_path)

        self.assertTrue(transfer.log.error.called)
        self.assertIn("failed", transfer.log.error.call_args[0][0])

        # cleanup
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # one iteration, error during receive, stopped
        # --------------------------------------------------------------------
        self.log.info("{}: ONE ITERATION, ERROR DURING RECEIVE, STOPPED"
                      .format(current_func_name))

        metadata = None
        payload = None

        mock_get_chunk.side_effect = TestException()
        transfer.stopped_everything = True
        mock_gen_filepath.reset_mock()

        transfer.store(target_base_path)

        self.assertFalse(mock_gen_filepath.called)

        # cleanup
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # one iteration, received and closed
        # --------------------------------------------------------------------
        self.log.info("{}: ONE ITERATION, RECEIVED AND CLOSE"
                      .format(current_func_name))

        metadata = {}
        payload = "foo"

        mock_get_chunk.side_effect = [[metadata, payload]]
        mock_gen_filepath.side_effect = ["test_target_filepath"]
        mock_store_chunk.side_effect = [False]

        transfer.store(target_base_path)

        self.assertTrue(mock_get_chunk.called)
        self.assertEqual(mock_get_chunk.call_count, 1)
        self.assertTrue(mock_gen_filepath.called)
        self.assertEqual(mock_gen_filepath.call_count, 1)
        self.assertTrue(mock_store_chunk.called)
        self.assertEqual(mock_store_chunk.call_count, 1)

        # cleanup
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_gen_filepath.side_effect = None
        mock_gen_filepath.reset_mock()
        mock_store_chunk.side_effect = None
        mock_store_chunk.reset_mock()

        # --------------------------------------------------------------------
        # receive and close in second iteration
        # --------------------------------------------------------------------
        self.log.info("{}: RECEIVE AND CLOSE IN SECOND ITERATION"
                      .format(current_func_name))

        metadata = {}
        payload = "foo"

        mock_get_chunk.side_effect = [[metadata, payload],
                                      [metadata, payload]]
        mock_gen_filepath.side_effect = ["test_target_filepath0",
                                         "test_target_filepath1"]
        mock_store_chunk.side_effect = [True, False]

        transfer.store(target_base_path)

        self.assertTrue(mock_get_chunk.called)
        self.assertEqual(mock_get_chunk.call_count, 2)
        self.assertTrue(mock_gen_filepath.called)
        self.assertEqual(mock_gen_filepath.call_count, 2)
        self.assertTrue(mock_store_chunk.called)
        self.assertEqual(mock_store_chunk.call_count, 2)

        # cleanup
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_gen_filepath.side_effect = None
        mock_gen_filepath.reset_mock()
        mock_store_chunk.side_effect = None
        mock_store_chunk.reset_mock()

        # --------------------------------------------------------------------
        # receive but error in storing
        # --------------------------------------------------------------------
        self.log.info("{}: RECEIVE BUT ERROR IN STORING"
                      .format(current_func_name))

        metadata = {}
        payload = "foo"

        mock_get_chunk.side_effect = [[metadata, payload]]
        mock_gen_filepath.side_effect = ["test_target_filepath"]
        mock_store_chunk.side_effect = TestException()

        transfer.status = [b"OK"]

        transfer.store(target_base_path)

        self.assertEqual(transfer.status[0], b"ERROR")

        self.assertTrue(mock_get_chunk.called)
        self.assertEqual(mock_get_chunk.call_count, 1)
        self.assertTrue(mock_gen_filepath.called)
        self.assertEqual(mock_gen_filepath.call_count, 1)
        self.assertTrue(mock_store_chunk.called)
        self.assertEqual(mock_store_chunk.call_count, 1)

        # cleanup
        mock_get_chunk.side_effect = None
        mock_get_chunk.reset_mock()
        mock_gen_filepath.side_effect = None
        mock_gen_filepath.reset_mock()
        mock_store_chunk.side_effect = None
        mock_store_chunk.reset_mock()

    def test__stop_socket(self):

        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # close socket
        # --------------------------------------------------------------------

        transfer.test_socket = mock.MagicMock()
        transfer.log = mock.MagicMock()
        transfer.log.info = mock.MagicMock()

        transfer._stop_socket("test_socket")

        self.assertIsNone(transfer.test_socket)
        transfer.log.info.assert_called_once_with("Closing test_socket")

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # socket already closed
        # --------------------------------------------------------------------

        transfer.test_socket = None
        transfer._stop_socket("test_socket")
        self.assertIsNone(transfer.test_socket)

        # --------------------------------------------------------------------
        # socket already closed
        # --------------------------------------------------------------------

        test_socket = mock.MagicMock()
        transfer.log = mock.MagicMock()

        ret_val = transfer._stop_socket("test_socket", test_socket)

        transfer.log.info.assert_called_once_with("Closing test_socket")
        self.assertIsNone(ret_val)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

    def test_stop(self):
        current_func_name = inspect.currentframe().f_code.co_name

        transfer = m_transfer.Transfer(**self.transfer_conf)

        mock_file = mock.MagicMock(close=mock.MagicMock())

        # --------------------------------------------------------------------
        # no open descriptors,
        # no signal exchange,
        # no control_socket,
        # no_auth,
        # no exteral context
        # --------------------------------------------------------------------
        self.log.info("{}: CLOSE SOCKETS".format(current_func_name))

        transfer.file_descriptors = {}
        transfer.signal_exchanged = None
        transfer.control_socket = None
        transfer.auth = None

        transfer.ext_context = True
        transfer.stopped_everything = False

        transfer.stop()

        self.assertIsNone(transfer.signal_socket)
        self.assertIsNone(transfer.data_socket)
        self.assertIsNone(transfer.request_socket)
        self.assertIsNone(transfer.status_check_socket)
        self.assertIsNone(transfer.confirmation_socket)
        self.assertIsNone(transfer.control_socket)

        self.assertTrue(transfer.stopped_everything)

        # --------------------------------------------------------------------
        # open descriptors
        # --------------------------------------------------------------------
        self.log.info("{}: OPEN DESCRIPTORS".format(current_func_name))

        transfer.signal_exchanged = None
        transfer.control_socket = None
        transfer.auth = None

        transfer.file_descriptors = {
            "test_filepath": {
                "file": mock_file
            }
        }

        transfer.stop()

        self.assertDictEqual(transfer.file_descriptors, {})

        # --------------------------------------------------------------------
        # signal exchanged
        # --------------------------------------------------------------------
        self.log.info("{}: SIGNAL EXCHANGED".format(current_func_name))

        transfer.file_descriptors = {}
        transfer.control_socket = None
        transfer.auth = None

        def stop_started_connections(transfer, connection):
            transfer.signal_socket = mock.MagicMock()
            transfer.signal_exchanged = b"{}".format(connection)
            transfer.started_connections = {
                connection: None
            }

            m_mock_send_signal = "hidra.transfer.Transfer._send_signal"
            with mock.patch(m_mock_send_signal) as mock_send_signal:
                with mock.patch("hidra.transfer.Transfer._stop_socket"):
                    transfer.stop()

                self.assertTrue(mock_send_signal.called)
                mock_send_signal.assert_called_once_with("STOP_" + connection)

            self.assertDictEqual(transfer.started_connections, {})

            # cleanup
            transfer.signal_socket = None

        def stop_signal_exchanged(transfer, connection):
            transfer.signal_socket = mock.MagicMock()
            transfer.signal_exchanged = connection
            transfer.started_connections = {}

            m_mock_send_signal = "hidra.transfer.Transfer._send_signal"
            with mock.patch(m_mock_send_signal) as mock_send_signal:
                with mock.patch("hidra.transfer.Transfer._stop_socket"):
                    transfer.stop()

                self.assertTrue(mock_send_signal.called)
                mock_send_signal.assert_called_once_with("STOP_" + connection)

            # cleanup
            transfer.signal_socket = None

        stop_started_connections(transfer, "STREAM")
        stop_signal_exchanged(transfer, b"STREAM")
        stop_started_connections(transfer, "QUERY_NEXT")
        stop_signal_exchanged(transfer, b"QUERY_NEXT")

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # control_socket
        # --------------------------------------------------------------------
        self.log.info("{}: CONTROL_SOCKET".format(current_func_name))

        transfer.file_descriptors = {}
        transfer.signal_exchanged = None
        transfer.auth = None

        transfer.control_socket = mock.MagicMock()
        transfer.control_conf = {"ipc_file": None}

        with mock.patch("hidra.transfer.Transfer._get_ipc_addr"):
            with mock.patch("hidra.transfer.Transfer._stop_socket"):
                with mock.patch("os.remove") as mock_remove:
                    transfer.stop()

                    self.assertTrue(mock_remove.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # control_socket, Exception
        # --------------------------------------------------------------------
        def stop_control_socket_with_exception(transfer, side_effect):
            # pylint: disable=invalid-name

            transfer.file_descriptors = {}
            transfer.signal_exchanged = None
            transfer.auth = None

            transfer.log = mock.MagicMock()
            transfer.control_socket = mock.MagicMock()
            transfer.control_conf = {"ipc_file": None}

            with mock.patch("hidra.transfer.Transfer._get_ipc_addr"):
                with mock.patch("hidra.transfer.Transfer._stop_socket"):
                    with mock.patch("os.remove") as mock_remove:
                        mock_remove.side_effect = side_effect

                        transfer.stop()

            self.assertTrue(transfer.log.warning.called)
            self.assertIn("not remove", transfer.log.warning.call_args[0][0])

            # cleanup
            transfer = m_transfer.Transfer(**self.transfer_conf)

        self.log.info("{}: CONTROL_SOCKET, OSERROR".format(current_func_name))
        stop_control_socket_with_exception(transfer, OSError())
        self.log.info("{}: CONTROL_SOCKET, EXCEPTION"
                      .format(current_func_name))
        stop_control_socket_with_exception(transfer, TestException())

        # --------------------------------------------------------------------
        # auth, ok
        # --------------------------------------------------------------------
        self.log.info("{}: AUTH".format(current_func_name))

        transfer.file_descriptors = {}
        transfer.signal_exchanged = None
        transfer.control_socket = None

        transfer.auth = mock.MagicMock()
        transfer.stop()
        self.assertIsNone(transfer.auth)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # auth, exception
        # --------------------------------------------------------------------
        self.log.info("{}: AUTH, EXCEPTION".format(current_func_name))

        transfer.file_descriptors = {}
        transfer.signal_exchanged = None
        transfer.control_socket = None

        transfer.auth = mock.MagicMock()
        transfer.auth.stop.side_effect = TestException()
        transfer.log = mock.MagicMock()

        transfer.stop()

        self.assertIsNotNone(transfer.auth)
        self.assertTrue(transfer.log.error.called)
        self.assertIn("Error", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # no external context
        # --------------------------------------------------------------------
        self.log.info("{}: NO EXTERNAL CONTEXT".format(current_func_name))

        transfer.file_descriptors = {}
        transfer.signal_exchanged = None
        transfer.control_socket = None
        transfer.auth = None

        transfer.ext_context = False
        mock_context = mock.MagicMock()
        transfer.context = mock_context

        transfer.stop()

        self.assertTrue(mock_context.destroy.called)
        self.assertIsNone(transfer.context)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # no external context, exception
        # --------------------------------------------------------------------
        self.log.info("{}: NO EXTERNAL CONTEXT, EXCEPTION"
                      .format(current_func_name))

        transfer.file_descriptors = {}
        transfer.signal_exchanged = None
        transfer.control_socket = None
        transfer.auth = None

        mock_context = mock.MagicMock()
        mock_context.destroy.side_effect = TestException()

        transfer.ext_context = False
        transfer.context = mock_context
        transfer.log = mock.MagicMock()

        transfer.stop()

        self.assertTrue(transfer.log.error.called)
        self.assertIn("failed", transfer.log.error.call_args[0][0])

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

    @mock.patch("hidra.transfer.Transfer._send_signal")
    @mock.patch("hidra.transfer.Transfer._set_targets")
    @mock.patch("hidra.transfer.Transfer._create_signal_socket")
    @mock.patch("hidra.transfer.Transfer.stop")
    def test_force_stop(self,
                        mock_stop,
                        mock_create_signal_socket,
                        mock_set_targets,
                        mock_send_signal):
        # pylint: disable=unused-argument

        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # targets of wrong format
        # --------------------------------------------------------------------

        with self.assertRaises(m_transfer.FormatError):
            transfer.force_stop(targets="")

        self.assertTrue(mock_stop.called)

        # --------------------------------------------------------------------
        # force stop ok
        # --------------------------------------------------------------------

        def call_force_stop(transfer, connection):
            transfer.connection_type = connection
            signal = b"FORCE_STOP_{}".format(connection)

            mock_send_signal.side_effect = [[signal]]
            transfer.log = mock.MagicMock()

            transfer.force_stop(targets=[])

            mock_send_signal.assert_called_once_with(signal)
            self.assertTrue(transfer.log.info.called)
            self.assertIn("Received", transfer.log.info.call_args[0][0])

            # cleanup
            transfer = m_transfer.Transfer(**self.transfer_conf)
            mock_send_signal.side_effect = None
            mock_send_signal.reset_mock()

        call_force_stop(transfer, "STREAM")
        call_force_stop(transfer, "STREAM_METADATA")
        call_force_stop(transfer, "QUERY_NEXT")
        call_force_stop(transfer, "QUERY_NEXT_METADATA")

        # --------------------------------------------------------------------
        # wrong response
        # --------------------------------------------------------------------

        transfer.connection_type = "STREAM"

        mock_send_signal.side_effect = [["foo"]]
        transfer.log = mock.MagicMock()

        transfer.force_stop(targets=[])

        mock_send_signal.assert_called_once_with(b"FORCE_STOP_STREAM")
        self.assertFalse(transfer.log.info.called)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)
        mock_send_signal.side_effect = None
        mock_send_signal.reset_mock()

        # --------------------------------------------------------------------
        # create context
        # --------------------------------------------------------------------

        transfer.ext_context = True
        transfer.context = None

        with mock.patch("zmq.Context"):
            transfer.force_stop([])

        self.assertIsInstance(transfer.context, mock.MagicMock)
        self.assertFalse(transfer.ext_context)

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

        # --------------------------------------------------------------------
        # create signal_socket
        # --------------------------------------------------------------------

        transfer.signal_socket = None
        mock_create_signal_socket.reset_mock()

        transfer.force_stop([])

        mock_create_signal_socket.assert_called_once_with()

        # cleanup
        transfer = m_transfer.Transfer(**self.transfer_conf)

    def tearDown(self):
        super(TestTransfer, self).tearDown()
