"""Testing the zmq_events event detector.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import copy
import inspect
import json
import mock
import os
import re
import time
import zmq
import logging
from multiprocessing import Queue

from .__init__ import BASE_DIR
from test_base import TestBase, create_dir
import hidra
import hidra.transfer as m_transfer

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestTransfer(TestBase):
    """Specification of tests to be performed for the loaded EventDetecor.
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

        exception_raised = False
        # self.assertRaises only works for unittest version >= 2.7
        try:
            transfer._setup(**self.transfer_conf)
        except m_transfer.NotSupported:
            exception_raised = True

        self.assertTrue(exception_raised)

    def test_get_remote_version(self):
        current_func_name = inspect.currentframe().f_code.co_name

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

        targets = ""
        exception_raised = False
        try:
            transfer.initiate(targets)
        except m_transfer.FormatError:
            exception_raised = True

        self.assertTrue(exception_raised)

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

                    exception_raised = True
                    try:
                        transfer.initiate(targets)
                    except m_transfer.CommunicationFailed:
                        exception_raised = True

                    self.assertTrue(exception_raised)

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

                exception_raised = False
                try:
                    transfer._create_signal_socket()
                except m_transfer.ConnectionFailed:
                    exception_raised = True

                self.assertTrue(mock_stop.called)
                self.assertTrue(exception_raised)

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
        targets = ""

        exception_raised = False
        try:
            transfer._set_targets(targets)
        except m_transfer.FormatError:
            exception_raised = True

        self.assertTrue(exception_raised)

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

        exception_raised = False
        try:
            transfer._set_targets(targets)
        except m_transfer.FormatError:
            exception_raised = True

        self.assertTrue(exception_raised)

    def test__send_signal(self):
        transfer = m_transfer.Transfer(**self.transfer_conf)

        class MockZmqSocket(mock.MagicMock):

            def __init__(self, **kwds):
                super(MockZmqSocket, self).__init__(**kwds)
                self._connected = False

                self.send_multipart = mock.MagicMock()
                self.recv_multipart = mock.MagicMock()

            def bind(self, endpoint):
                assert not self._connected
                assert endpoint != ""
                self._connected = True

            def connect(self, endpoint):
                assert not self._connected
                assert endpoint != ""
                self._connected = True

            def close(self, linger):
                assert self._connected
                self._connected = False



        # --------------------------------------------------------------------
        # no signal
        # --------------------------------------------------------------------
        ret_val = transfer._send_signal(None)

        self.assertIsNone(ret_val)

        # --------------------------------------------------------------------
        # Error when sending
        # --------------------------------------------------------------------
        class TestException(Exception):
            pass

        transfer.signal_socket = MockZmqSocket()
        transfer.signal_socket.send_multipart.side_effect = TestException()

        exception_raised = False
        try:
            transfer._send_signal("foo")
        except TestException:
            exception_raised = True

        self.assertTrue(exception_raised)

        # cleanup
        transfer.signal_socket = None

    def todo_test__get_data_endpoint(self):
        pass

    def todo_test__update_ip(self):
        pass

    def todo_test__get_endpoint(self):
        pass

    def todo_test__get_tcp_addr(self):
        pass

    def todo_test__get_ipc_addr(self):
        pass

    def todo_test_start(self):
        pass

    def todo_test_setopt(self):
        pass

    def todo_test__unpack_value(self):
        pass

    def todo_test_register(self):
        pass

    def todo_test_read(self):
        pass

    def todo_test__react_on_message(self):
        pass

    def todo_test_get_chunk(self):
        pass

    def todo_test_check_file_closed(self):
        pass

    def todo_test_get(self):
        pass

    def todo_test_store_data_chunk(self):
        pass

    def todo_test_store(self):
        pass

    def todo_test_stop(self):
        pass

    def todo_test_force_stop(self):
        pass

    def tearDown(self):
        super(TestTransfer, self).tearDown()
