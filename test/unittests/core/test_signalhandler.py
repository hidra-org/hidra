"""Testing the task provider.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import inspect
import json
import logging
import mock
import os
import re
import threading
import time
import zmq
from multiprocessing import freeze_support
from six import iteritems

import utils
from .__init__ import BASE_DIR
from test_base import TestBase, create_dir, MockLogging, mock_get_logger
from signalhandler import SignalHandler, UnpackedMessage, TargetProperties
from _version import __version__

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class RequestPuller(threading.Thread):
    def __init__(self, endpoints, log_queue):
        threading.Thread.__init__(self)

        self.log = utils.get_logger("RequestPuller", log_queue)
        self.continue_run = True

        self.context = zmq.Context()

        self.request_fw_socket = utils.start_socket(
            name="request_fw_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint=endpoints.request_fw_con,
            context=self.context,
            log=self.log
        )

    def run(self):
        self.log.info("Start run")
        filename = "test_file.cbf"
        while self.continue_run:
            try:
                msg = json.dumps(filename).encode("utf-8")
                self.request_fw_socket.send_multipart([b"GET_REQUESTS", msg])
                self.log.info("send {}".format(msg))
            except Exception as e:
                raise

            try:
                requests = json.loads(self.request_fw_socket.recv_string())
                self.log.info("Requests: {}".format(requests))
                time.sleep(0.25)
            except Exception as e:
                raise

    def stop(self):
        if self.continue_run:
            self.log.info("Shutdown RequestPuller")
            self.continue_run = False

        if self.request_fw_socket is not None:
            self.log.info("Closing request_fw_socket")
            self.request_fw_socket.close(0)
            self.request_fw_socket = None

            self.context.term()

    def __exit__(self):
        self.stop()


class TestSignalHandler(TestBase):
    """Specification of tests to be performed for the TaskProvider.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestSignalHandler, self).setUp()

        # see https://docs.python.org/2/library/multiprocessing.html#windows
        freeze_support()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        # Register context
        self.context = zmq.Context()

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self.local_target = os.path.join(BASE_DIR, "data", "target")
        self.chunksize = 10485760  # = 1024*1024*10 = 10 MiB

        general_config = {
            "store_data": False
        }

        self.receiving_ports = [6005, 6006]

        self.signalhandler_config = {
            "config": general_config,
            "endpoints": self.config["endpoints"],
            "whitelist": None,
            "ldapuri": "it-ldap-slave.desy.de:1389",
            "log_queue": self.log_queue,
            "context": self.context
        }

    def send_signal(self, socket, signal, ports, prio=None):
        self.log.info("send_signal : {}, {}".format(signal, ports))

        app_id = str(self.config["main_pid"]).encode("utf-8")

        send_message = [__version__, app_id, signal]

        targets = []
        if type(ports) == list:
            for port in ports:
                targets.append(["{}:{}".format(self.con_ip, port), prio, [""]])
        else:
            targets.append(["{}:{}".format(self.con_ip, ports), prio, [""]])

        targets = json.dumps(targets).encode("utf-8")
        send_message.append(targets)

        socket.send_multipart(send_message)

        received_message = socket.recv()
        self.log.info("Responce : {}".format(received_message))

    def send_request(self, socket, socket_id):
        send_message = [b"NEXT", socket_id.encode('utf-8')]
        self.log.info("send_request: {}".format(send_message))
        socket.send_multipart(send_message)
        self.log.info("request sent: {}".format(send_message))

    def cancel_request(self, socket, socket_id):
        send_message = [b"CANCEL", socket_id.encode('utf-8')]
        self.log.info("send_request: {}".format(send_message))
        socket.send_multipart(send_message)
        self.log.info("request sent: {}".format(send_message))

    @mock.patch.object(utils, "get_logger", mock_get_logger)
    def test_signalhandler(self):
        """Simulate incoming data and check if received events are correct.
        """

        endpoints = self.config["endpoints"]

        receiving_endpoints = []
        for port in self.receiving_ports:
            receiving_endpoints.append("{}:{}".format(self.con_ip, port))

        # create control socket
        # control messages are not send over an forwarder, thus the
        # control_sub endpoint is used directly
        control_pub_socket = self.start_socket(
            name="control_pub_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            endpoint=endpoints.control_sub_bind
        )

        self.signalhandler_config["context"] = None
        signalhandler_thr = threading.Thread(target=SignalHandler,
                                             kwargs=self.signalhandler_config)
        signalhandler_thr.start()

        # to give the signal handler to bind to the socket before the connect
        # is done
        time.sleep(0.5)

        request_puller_thr = RequestPuller(endpoints,
                                           self.log_queue)
        request_puller_thr.start()

        com_socket = self.start_socket(
            name="com_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint=endpoints.com_con
        )

        request_socket = self.start_socket(
            name="request_socket",
            sock_type=zmq.PUSH,
            sock_con="connect",
            endpoint=endpoints.request_con
        )

        time.sleep(1)

        try:
            self.send_signal(socket=com_socket,
                             signal=b"START_STREAM",
                             ports=6003,
                             prio=1)

            self.send_signal(socket=com_socket,
                             signal=b"START_STREAM",
                             ports=6004,
                             prio=0)

            self.send_signal(socket=com_socket,
                             signal=b"STOP_STREAM",
                             ports=6003)

            self.send_request(socket=request_socket,
                              socket_id=receiving_endpoints[1])

            self.send_signal(socket=com_socket,
                             signal=b"START_QUERY_NEXT",
                             ports=self.receiving_ports,
                             prio=2)

            self.send_request(socket=request_socket,
                              socket_id=receiving_endpoints[1].encode())
            self.send_request(socket=request_socket,
                              socket_id=receiving_endpoints[1].encode())
            self.send_request(socket=request_socket,
                              socket_id=receiving_endpoints[0].encode())

            self.cancel_request(socket=request_socket,
                                socket_id=receiving_endpoints[1].encode())

            time.sleep(0.5)

            self.send_request(socket=request_socket,
                              socket_id=receiving_endpoints[0])
            self.send_signal(socket=com_socket,
                             signal=b"STOP_QUERY_NEXT",
                             ports=self.receiving_ports[0],
                             prio=2)

            time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:

            control_pub_socket.send_multipart([b"control", b"EXIT"])
            self.log.debug("Sent control signal EXIT")

#            signalhandler_thr.stop()
            signalhandler_thr.join()
            request_puller_thr.stop()
            request_puller_thr.join()

            self.stop_socket(name="com_socket", socket=com_socket)
            self.stop_socket(name="request_socket", socket=request_socket)
            self.stop_socket(name="control_pub_socket",
                             socket=control_pub_socket)

    # mocking of stop has to be done for the whole function because otherwise
    # it is called in __del__
    @mock.patch("signalhandler.SignalHandler.stop")
    def test_setup(self, mock_stop):
        current_func_name = inspect.currentframe().f_code.co_name

        with mock.patch("signalhandler.SignalHandler.setup"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        conf = self.signalhandler_config

        setup_conf = dict(
            log_queue=conf["log_queue"],
            context=conf["context"],
            whitelist=conf["whitelist"],
            ldapuri=conf["ldapuri"]
        )

        # --------------------------------------------------------------------
        # external context
        # --------------------------------------------------------------------
        self.log.info("{}: EXTERNAL CONTEXT".format(current_func_name))

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            sighandler.setup(**setup_conf)

        self.assertIsInstance(sighandler.log, logging.Logger)
        self.assertEqual(sighandler.whitelist, conf["whitelist"])
        self.assertEqual(sighandler.context, conf["context"])
        self.assertTrue(sighandler.ext_context)

        # resetting QueueHandlers
        sighandler.log.handlers = []

        # --------------------------------------------------------------------
        # no external context
        # --------------------------------------------------------------------
        self.log.info("{}: NO EXTERNAL CONTEXT".format(current_func_name))

        setup_conf["context"] = None
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            sighandler.setup(**setup_conf)

        self.assertIsInstance(sighandler.log, logging.Logger)
        self.assertEqual(sighandler.whitelist, conf["whitelist"])
        self.assertIsInstance(sighandler.context, zmq.Context)
        self.assertFalse(sighandler.ext_context)

    @mock.patch("signalhandler.SignalHandler.stop")
    def test_create_sockets(self, mock_stop):
        current_func_name = inspect.currentframe().f_code.co_name

        class MockZmqPoller(mock.MagicMock):

            def __init__(self, **kwds):
                super(MockZmqPoller, self).__init__(**kwds)
                self.registered_sockets = []

            def register(self, socket, event):
                assert isinstance(socket, zmq.sugar.socket.Socket)
                assert event in [zmq.POLLIN, zmq.POLLOUT, zmq.POLLERR]
                self.registered_sockets.append([socket, event])

        def init():
            with mock.patch("signalhandler.SignalHandler.create_sockets"):
                with mock.patch("signalhandler.SignalHandler.exec_run"):
                    sighandler = SignalHandler(**self.signalhandler_config)

            with mock.patch.object(zmq, "Poller", MockZmqPoller):
                sighandler.create_sockets()

            return sighandler

        def check_registered(sockets, testunit):
            registered_sockets = sighandler.poller.registered_sockets
            all_socket_confs = []

            # check that sockets are registered
            for socket in sockets:
                socket_conf = [socket, zmq.POLLIN]
                testunit.assertIn(socket_conf, registered_sockets)
                all_socket_confs.append(socket_conf)

            # check that they are the only ones registered
            testunit.assertEqual(all_socket_confs, registered_sockets)

        zmq_socket = zmq.sugar.socket.Socket

        # --------------------------------------------------------------------
        # with no nodes allowed to connect
        # --------------------------------------------------------------------
        self.log.info("{}: WITH NO NODES ALLOWED TO CONNECT"
                      .format(current_func_name))

        self.signalhandler_config["whitelist"] = []
        sighandler = init()

        self.assertIsInstance(sighandler.control_pub_socket, zmq_socket)
        self.assertIsInstance(sighandler.control_sub_socket, zmq_socket)
        self.assertEqual(sighandler.com_socket, None)
        self.assertEqual(sighandler.request_socket, None)

        sockets_to_test = [
            sighandler.control_sub_socket,
            sighandler.request_fw_socket
        ]
        check_registered(sockets_to_test, self)

        # resetting QueueHandlers
        sighandler.log.handlers = []

        # --------------------------------------------------------------------
        # with all nodes allowed to connect
        # --------------------------------------------------------------------
        self.log.info("{}: WITH ALL NODES ALLOWED TO CONNECT"
                      .format(current_func_name))

        self.signalhandler_config["whitelist"] = None
        sighandler = init()

        self.assertIsInstance(sighandler.control_pub_socket, zmq_socket)
        self.assertIsInstance(sighandler.control_sub_socket, zmq_socket)
        self.assertIsInstance(sighandler.request_fw_socket, zmq_socket)
        self.assertIsInstance(sighandler.com_socket, zmq_socket)
        self.assertIsInstance(sighandler.request_socket, zmq_socket)

        sockets_to_test = [
            sighandler.control_sub_socket,
            sighandler.request_fw_socket,
            sighandler.com_socket,
            sighandler.request_socket
        ]
        check_registered(sockets_to_test, self)

    def test_run(self):
        current_func_name = inspect.currentframe().f_code.co_name

        # with all nodes allowed to connect
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.com_socket = mock.MagicMock()
        sighandler.request_socket = mock.MagicMock()
        sighandler.request_fw_socket = mock.MagicMock()
        sighandler.control_sub_socket = mock.MagicMock()
        sighandler.control_sub_socket.recv_multipart = mock.MagicMock()
        sighandler.poller = mock.MagicMock(spec_set=zmq.Poller)
        sighandler.poller.poll = mock.MagicMock()

        def init_sighandler(sighandler, socket, signal):

            sighandler.poller.poll.side_effect = [
                {socket: zmq.POLLIN},
                # for stopping the run loop
                {sighandler.control_sub_socket: zmq.POLLIN}
            ]
            socket.recv_multipart.side_effect = [signal]
            # either use side_effect or recreate mock object and use
            # return_value
            sighandler.control_sub_socket.recv_multipart.side_effect = [
                ["", "EXIT"]
            ]

        def reset(sighandler):
            sighandler.com_socket.reset_mock()
            sighandler.request_socket.reset_mock()
            sighandler.request_fw_socket.reset_mock()
            sighandler.control_sub_socket.reset_mock()
            sighandler.control_sub_socket.recv_multipart.reset_mock()
            sighandler.poller.poll.reset_mock()

        host = self.con_ip
        port = 1234
        send_type = "data"

        # --------------------------------------------------------------------
        # control_sub_socket: exit
        # --------------------------------------------------------------------
        self.log.info("{}: CONTROL_SUB_SOCKET: EXIT"
                      .format(current_func_name))

        sighandler.log = mock.MagicMock()

        sighandler.poller.poll.side_effect = [
            {sighandler.control_sub_socket: zmq.POLLIN}
        ]
        sighandler.control_sub_socket.recv_multipart.return_value = ["",
                                                                     "EXIT"]

        sighandler.run()

        calls = sighandler.log.method_calls
        expected = mock.call.info("Requested to shutdown.")
        self.assertIn(expected, calls)

        # reset
        sighandler.log = MockLogging()
        reset(sighandler)

        # --------------------------------------------------------------------
        # control_sub_socket: SLEEP/WAKEUP or unhandled
        # --------------------------------------------------------------------
        self.log.info("{}: CONTROL_SUB_SOCKET: SLEEP/WAKEUP"
                      .format(current_func_name))

        sighandler.poller.poll.side_effect = [
            {sighandler.control_sub_socket: zmq.POLLIN},
            # for stopping the run loop
            {sighandler.control_sub_socket: zmq.POLLIN}
        ]
        sighandler.control_sub_socket.recv_multipart.side_effect = [
            ["", "SLEEP"],
            # for stopping the run loop
            ["", "EXIT"],
        ]

        sighandler.run()

        # if the code run till here without throwing an exception if passed
        # if StopIteration is thrown by mock that means that poll was called
        # more often than 2 times

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_fw_socket: failed receive
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_FW_SOCKET: FAILED RECEIVE"
                      .format(current_func_name))

        signal = [Exception()]
        init_sighandler(sighandler, sighandler.request_fw_socket, signal)
        sighandler.log.error = mock.MagicMock()

        sighandler.run()

        call = sighandler.log.error.call_args[0][0]
        self.log.debug("call args {}".format(call))
        expected = "Failed to receive/answer"
        self.assertIn(expected, call)

        # reset
        sighandler.log = MockLogging()
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_fw_socket: incoming message not supported
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_FW_SOCKET: INCOMING MESSAGE NOT SUPPORTED"
                      .format(current_func_name))

        signal = ["NOT_SUPPORTED"]
        init_sighandler(sighandler, sighandler.request_fw_socket, signal)
        sighandler.log.error = mock.MagicMock()

        sighandler.run()

        call = sighandler.log.error.call_args[0][0]
        self.log.debug("call args {}".format(call))
        expected = "Failed to receive/answer"
        self.assertIn(expected, call)

        # reset
        sighandler.log = MockLogging()
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_fw_socket: no open requests
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_FW_SOCKET: NO OPEN REQUESTS"
                      .format(current_func_name))

        signal = [b"GET_REQUESTS",
                  json.dumps({"filename": "my_filename"}).encode("utf-8")]
        init_sighandler(sighandler, sighandler.request_fw_socket, signal)
        sighandler.request_fw_socket.send_string = mock.MagicMock()

        sighandler.registered_streams = []
        sighandler.vari_requests = []
        sighandler.perm_requests = []

        sighandler.run()

        expected = json.dumps(["None"])
        (sighandler.request_fw_socket
         .send_string
         .assert_called_once_with(expected))

        self.assertEqual(sighandler.registered_streams, [])
        self.assertEqual(sighandler.vari_requests, [])
        self.assertEqual(sighandler.perm_requests, [])

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_fw_socket: open perm requests (match)
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_FW_SOCKET: OPEN PERM REQUESTS (MATCH)"
                      .format(current_func_name))
        signal = [b"GET_REQUESTS",
                  json.dumps("my_filename").encode("utf-8")]
        init_sighandler(sighandler, sighandler.request_fw_socket, signal)
        sighandler.request_fw_socket.send_string = mock.MagicMock()

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        sighandler.registered_streams = [
            TargetProperties(targets=targets, appid=None)
        ]
        sighandler.vari_requests = []
        sighandler.perm_requests = [0]

        sighandler.run()

        expected = json.dumps([["{}:{}".format(host, port), 0, send_type]])
        (sighandler.request_fw_socket
         .send_string
         .assert_called_once_with(expected))

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        expected = [
            TargetProperties(targets=targets, appid=None)
        ]
        self.assertEqual(sighandler.registered_streams, expected)
        self.assertEqual(sighandler.vari_requests, [])
        self.assertEqual(sighandler.perm_requests, [0])

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_fw_socket: open perm requests (no match)
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_FW_SOCKET: OPEN PERM REQUESTS (NO MATCH)"
                      .format(current_func_name))

        signal = [b"GET_REQUESTS",
                  json.dumps("my_filename").encode("utf-8")]
        init_sighandler(sighandler, sighandler.request_fw_socket, signal)
        sighandler.request_fw_socket.send_string = mock.MagicMock()

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".py"), send_type]
        ]
        sighandler.registered_streams = [
            TargetProperties(targets=targets, appid=None)
        ]

        sighandler.vari_requests = []
        sighandler.perm_requests = [0]

        sighandler.run()

        expected = json.dumps(["None"])
        (sighandler.request_fw_socket
         .send_string
         .assert_called_once_with(expected))

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".py"), send_type]
        ]
        expected = [
            TargetProperties(targets=targets, appid=None)
        ]
        self.assertEqual(sighandler.registered_streams, expected)
        self.assertEqual(sighandler.vari_requests, [])
        self.assertEqual(sighandler.perm_requests, [0])

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_fw_socket: open vari requests (match)
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_FW_SOCKET: OPEN VARI REQUESTS (MATCH)"
                      .format(current_func_name))

        signal = [b"GET_REQUESTS",
                  json.dumps("my_filename").encode("utf-8")]
        init_sighandler(sighandler, sighandler.request_fw_socket, signal)
        sighandler.request_fw_socket.send_string = mock.MagicMock()

        sighandler.registered_streams = []
        sighandler.vari_requests = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]
        sighandler.perm_requests = []

        sighandler.run()

        expected = json.dumps([["{}:{}".format(host, port), 0, send_type]])
        (sighandler.request_fw_socket
         .send_string
         .assert_called_once_with(expected))

        self.assertEqual(sighandler.registered_streams, [])
        self.assertEqual(sighandler.vari_requests, [[]])
        self.assertEqual(sighandler.perm_requests, [])

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_fw_socket: open vari requests (no match)
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_FW_SOCKET: OPEN VARI REQUESTS (NO MATCH)"
                      .format(current_func_name))

        signal = [b"GET_REQUESTS",
                  json.dumps("my_filename").encode("utf-8")]
        init_sighandler(sighandler, sighandler.request_fw_socket, signal)
        sighandler.request_fw_socket.send_string = mock.MagicMock()

        sighandler.registered_streams = []
        sighandler.vari_requests = [
            [["{}:{}".format(host, port), 0, re.compile(".py"), send_type]]
        ]
        sighandler.perm_requests = []

        sighandler.run()

        expected = json.dumps(["None"])
        (sighandler.request_fw_socket
         .send_string
         .assert_called_once_with(expected))

        expected = [
            [["{}:{}".format(host, port), 0, re.compile(".py"), send_type]]
        ]
        self.assertEqual(sighandler.registered_streams, [])
        self.assertEqual(sighandler.vari_requests, expected)
        self.assertEqual(sighandler.perm_requests, [])

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # com_socket: signal ok
        # --------------------------------------------------------------------
        self.log.info("{}: COM_SOCKET: SIGNAL OK".format(current_func_name))

        signal = []
        init_sighandler(sighandler, sighandler.com_socket, signal)
        sighandler.com_socket.recv_multipart = mock.MagicMock()
        sighandler.react_to_signal = mock.MagicMock()
        sighandler.send_response = mock.MagicMock()
        sighandler.check_signal = mock.MagicMock()
        sighandler.check_signal.return_value = UnpackedMessage(
            check_successful=True,
            response=None,
            appid=None,
            signal=None,
            targets=None
        )

        sighandler.run()

        self.assertTrue(sighandler.react_to_signal.called)

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # com_socket: signal not ok
        # --------------------------------------------------------------------
        self.log.info("{}: COM_SOCKET: SIGNAL NOT OK"
                      .format(current_func_name))

        signal = []
        init_sighandler(sighandler, sighandler.com_socket, signal)
        sighandler.com_socket.recv_multipart = mock.MagicMock()
        sighandler.react_to_signal = mock.MagicMock()
        sighandler.send_response = mock.MagicMock()
        sighandler.check_signal = mock.MagicMock()
        sighandler.check_signal.return_value = UnpackedMessage(
            check_successful=False,
            response=None,
            appid=None,
            signal=None,
            targets=None
        )

        sighandler.run()

        self.assertTrue(sighandler.send_response.called)

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_socket: not supported
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_SOCKET: NOT SUPPORTED"
                      .format(current_func_name))

        signal = [""]
        init_sighandler(sighandler, sighandler.request_socket, signal)
        sighandler.log = mock.MagicMock()

        sighandler.run()

        calls = sighandler.log.method_calls
        expected = mock.call.info("Request not supported.")
        self.assertIn(expected, calls)

        # reset
        sighandler.log = MockLogging()
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_socket: next (allowed)
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_SOCKET: NEXT (ALLOWED)"
                      .format(current_func_name))

        socket_id = "{}:{}".format(host, port)
        signal = ["NEXT", socket_id]
        init_sighandler(sighandler, sighandler.request_socket, signal)

        targets = [
            [socket_id, 0, re.compile(".*"), send_type]
        ]
        sighandler.registered_queries = [
            TargetProperties(targets=targets, appid=None)
        ]
        sighandler.vari_requests = [[]]

        with mock.patch("utils.convert_socket_to_fqdn") as mock_utils:
            mock_utils.return_value = socket_id
            sighandler.run()

        expected = [
            [[socket_id, 0, re.compile(".*"), send_type]]
        ]
        self.assertEqual(sighandler.vari_requests, expected)

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_socket: next (not allowed)
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_SOCKET: NEXT (NOT ALLOWED)"
                      .format(current_func_name))

        socket_id = "{}:{}".format(host, port)
        signal = ["NEXT", socket_id]
        init_sighandler(sighandler, sighandler.request_socket, signal)

        sighandler.registered_queries = []
        sighandler.vari_requests = []

        with mock.patch("utils.convert_socket_to_fqdn") as mock_utils:
            mock_utils.return_value = socket_id
            sighandler.run()

        self.assertEqual(sighandler.vari_requests, [])

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_socket: cancel (no requests)
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_SOCKET: CANCEL (NO REQUESTS)"
                      .format(current_func_name))

        socket_id = "{}:{}".format(host, port)
        signal = ["CANCEL", socket_id]
        init_sighandler(sighandler, sighandler.request_socket, signal)

        sighandler.registered_queries = []
        sighandler.vari_requests = []

        with mock.patch("utils.convert_socket_to_fqdn") as mock_utils:
            mock_utils.return_value = socket_id
            sighandler.run()

        self.assertEqual(sighandler.vari_requests, [])

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_socket: cancel (requests)
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_SOCKET: CANCEL (NO REQUESTS)"
                      .format(current_func_name))

        socket_id = "{}:{}".format(host, port)
        signal = ["CANCEL", socket_id]
        init_sighandler(sighandler, sighandler.request_socket, signal)

        sighandler.registered_queries = []
        sighandler.vari_requests = [
            [[socket_id, 0, re.compile(".*"), send_type]]
        ]

        with mock.patch("utils.convert_socket_to_fqdn") as mock_utils:
            mock_utils.return_value = socket_id
            sighandler.run()

        self.assertEqual(sighandler.vari_requests, [[]])

        # reset
        reset(sighandler)

        # --------------------------------------------------------------------
        # request_socket: cancel (multiple requests)
        # --------------------------------------------------------------------
        self.log.info("{}: REQUEST_SOCKET: CANCEL (NO REQUESTS)"
                      .format(current_func_name))

        socket_id = "{}:{}".format(host, port)
        signal = ["CANCEL", socket_id]
        init_sighandler(sighandler, sighandler.request_socket, signal)

        port2 = 9876

        sighandler.registered_queries = []
        sighandler.vari_requests = [
            [[socket_id, 0, re.compile(".*"), send_type],
             ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]]
        ]

        with mock.patch("utils.convert_socket_to_fqdn") as mock_utils:
            mock_utils.return_value = socket_id
            sighandler.run()

        expected = [
            [["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]]
        ]
        self.assertEqual(sighandler.vari_requests, expected)

        # reset
        reset(sighandler)

    def test_check_signal(self):
        current_func_name = inspect.currentframe().f_code.co_name

        # with all nodes allowed to connect
        self.signalhandler_config["whitelist"] = None
        with mock.patch("signalhandler.SignalHandler.setup"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.log = MockLogging()

        host = self.con_ip
        port = 1234
        appid = 1234567890
        appid = str(appid).encode("utf-8")
        in_targets_list = [["{}:{}".format(host, port), 0, [""]]]
        in_targets = json.dumps(in_targets_list).encode("utf-8")

        # --------------------------------------------------------------------
        # no valid message
        # --------------------------------------------------------------------
        self.log.info("{}: NO VALID MESSAGE".format(current_func_name))

        in_message = []
        unpacked_message = sighandler.check_signal(in_message)
        self.assertFalse(unpacked_message.check_successful)
        self.assertEqual(unpacked_message.response, [b"NO_VALID_SIGNAL"])
        self.assertIsNone(unpacked_message.appid)
        self.assertIsNone(unpacked_message.signal)
        self.assertIsNone(unpacked_message.targets)

        # --------------------------------------------------------------------
        # no valid message due to missing port
        # --------------------------------------------------------------------
        self.log.info("{}: NO VALID MESSAGE DUE TO MISSING PORT"
                      .format(current_func_name))

        fake_in_targets = [["{}".format(host), 0, [""]]]
        fake_in_targets = json.dumps(fake_in_targets).encode("utf-8")

        in_message = [__version__, appid, "START_STREAM", fake_in_targets]
        unpacked_message = sighandler.check_signal(in_message)
        self.assertFalse(unpacked_message.check_successful)
        self.assertEqual(unpacked_message.response, [b"NO_VALID_SIGNAL"])
        self.assertIsNone(unpacked_message.appid)
        self.assertIsNone(unpacked_message.signal)
        self.assertIsNone(unpacked_message.targets)

        # --------------------------------------------------------------------
        # no version set
        # --------------------------------------------------------------------
        self.log.info("{}: NO VERSION SET".format(current_func_name))

        in_message = [None, appid, "START_STREAM", in_targets]
        unpacked_message = sighandler.check_signal(in_message)
        self.assertFalse(unpacked_message.check_successful)
        self.assertEqual(unpacked_message.response, [b"NO_VALID_SIGNAL"])
        self.assertIsNone(unpacked_message.appid)
        self.assertIsNone(unpacked_message.signal)
        self.assertIsNone(unpacked_message.targets)

        # --------------------------------------------------------------------
        # valid message but version conflict
        # --------------------------------------------------------------------
        self.log.info("{}: VALID MESSAGE BUT VERSION CONFLICT"
                      .format(current_func_name))

        version = "0.0.0"
        #             version, application id, signal, targets
        in_message = [version, appid, "START_STREAM", in_targets]
        unpacked_message = sighandler.check_signal(in_message)
        self.assertFalse(unpacked_message.check_successful)
        self.assertEqual(unpacked_message.response,
                         ["VERSION_CONFLICT", __version__])
        self.assertIsNone(unpacked_message.appid)
        self.assertIsNone(unpacked_message.signal)
        self.assertIsNone(unpacked_message.targets)

        # --------------------------------------------------------------------
        # no version set (empty string)
        # --------------------------------------------------------------------
        self.log.info("{}: NO VERSION SET (EMPTY STRING)"
                      .format(current_func_name))

        in_message = ["", appid, "START_STREAM", in_targets]
        unpacked_message = sighandler.check_signal(in_message)
        self.assertTrue(unpacked_message.check_successful)
        self.assertIsNone(unpacked_message.response)
        self.assertEqual(unpacked_message.appid, appid)
        self.assertEqual(unpacked_message.signal, "START_STREAM")
        self.assertEqual(unpacked_message.targets, in_targets_list)

        # --------------------------------------------------------------------
        # valid message, valid version
        # --------------------------------------------------------------------
        self.log.info("{}: VALID MESSAGE, VALID VERSION"
                      .format(current_func_name))

        #             version, application id, signal, targets
        in_message = [__version__, appid, "START_STREAM", in_targets]
        unpacked_message = sighandler.check_signal(in_message)
        self.assertTrue(unpacked_message.check_successful)
        self.assertIsNone(unpacked_message.response)
        self.assertEqual(unpacked_message.appid, appid)
        self.assertEqual(unpacked_message.signal, "START_STREAM")
        self.assertEqual(unpacked_message.targets, in_targets_list)

        # resetting QueueHandlers
        sighandler.log.handlers = []

        # with no nodes allowed to connect
        self.signalhandler_config["whitelist"] = []
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.log = MockLogging()

        # --------------------------------------------------------------------
        # valid message, valid version, but host is not allowed to connect
        # --------------------------------------------------------------------
        self.log.info("{}: VALID MESSAGE, VALID VERSION, BUT HOST IS NOT "
                      "ALLOWED TO CONNTECT".format(current_func_name))

        #             version, application id, signal, targets
        in_message = [__version__, appid, "START_STREAM", in_targets]
        unpacked_message = sighandler.check_signal(in_message)
        self.assertFalse(unpacked_message.check_successful)
        self.assertEqual(unpacked_message.response, [b"NO_VALID_HOST"])
        self.assertIsNone(unpacked_message.appid)
        self.assertIsNone(unpacked_message.signal)
        self.assertIsNone(unpacked_message.targets)

    def test_send_response(self):
        current_func_name = inspect.currentframe().f_code.co_name

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.com_socket = mock.MagicMock(
            spec_set=zmq.sugar.socket.Socket
        )

        mocked_func = sighandler.com_socket.send_multipart

        # --------------------------------------------------------------------
        # signal is string
        # --------------------------------------------------------------------
        self.log.info("{}: SIGNAL IS STRING".format(current_func_name))

        signal = "test"
        sighandler.send_response(signal)

        # assert_called_once only works version >3.6
        self.assertTrue(mocked_func.call_count == 1)

        args, kwargs = mocked_func.call_args
        # mock returns a tuple with the args
        self.assertTrue(args[0] == [signal])

        mocked_func.reset_mock()

        # --------------------------------------------------------------------
        # signal is list
        # --------------------------------------------------------------------
        self.log.info("{}: SIGNAL IS LIST".format(current_func_name))

        signal = ["test"]
        sighandler.send_response(signal)

        # assert_called_once only works version >3.6
        self.assertTrue(mocked_func.call_count == 1)

        args, kwargs = mocked_func.call_args
        # mock returns a tuple with the args
        self.assertTrue(args[0] == signal)

    def test__start_signal(self):
        current_func_name = inspect.currentframe().f_code.co_name

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.send_response = mock.MagicMock()

        signal = b"START_STREAM"
        send_type = "metadata"
        host = self.con_ip
        port = 1234
        appid = 1234567890

        # --------------------------------------------------------------------
        # check that socket_id is added
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK THAT SOCKET_ID IS ADDED"
                      .format(current_func_name))

        #            [[<host>, <prio>, <suffix>], ...]
        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        registered_ids = []
        vari_requests = []
        perm_requests = []

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        expected_result = [TargetProperties(targets=targets, appid=appid)]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[]])
        self.assertListEqual(perm_requests, [0])

        # --------------------------------------------------------------------
        # check that same socke_id is not added twice
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK THAT SAME SOCKET_ID IS NOT ADDED TWICE"
                      .format(current_func_name))

        #            [[<host>, <prio>, <suffix>], ...]
        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        # there have to be entries in these two lists as well because len of
        # registered_ids, vari_requests and perm_requests should be the same
        vari_requests = [[]]
        perm_requests = [0]

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        expected_result = [TargetProperties(targets=targets, appid=appid)]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[]])
        self.assertListEqual(perm_requests, [0])

        # --------------------------------------------------------------------
        # adding additional set
        # --------------------------------------------------------------------
        self.log.info("{}: ADDING ADDITIONAL SET"
                      .format(current_func_name))

        host2 = "abc"
        port2 = 9876
        socket_ids = [["{}:{}".format(host, port), 0, ".*"],
                      ["{}:{}".format(host, port2), 0, ".*"]]

        targets = [
            ["{}:{}".format(host2, port), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        vari_requests = [[]]
        perm_requests = [0]

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        targets = [
            ["{}:{}".format(host2, port), 0, re.compile(".*"), send_type]
        ]
        targets2 = sorted([
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type],
            ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
        ])
        expected_result = [
            TargetProperties(targets=targets, appid=appid),
            TargetProperties(targets=targets2, appid=appid)
        ]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[], []])
        self.assertListEqual(perm_requests, [0, 0])

        # --------------------------------------------------------------------
        # adding additional set (superset of already existing one)
        # --------------------------------------------------------------------
        self.log.info("{}: ADDING ADDITIONAL SET (SUPERSET OF ALREADY "
                      "EXISTING ONE".format(current_func_name))

        socket_ids = [["{}:{}".format(host, port), 0, ".*"],
                      ["{}:{}".format(host, port2), 0, ".*"]]

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        vari_requests = [[]]
        perm_requests = [0]

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        targets = sorted([
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type],
            ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
        ])
        expected_result = [TargetProperties(targets=targets, appid=appid)]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[]])
        self.assertListEqual(perm_requests, [0])

        # --------------------------------------------------------------------
        # adding additional set (subset of already existing one)
        # --------------------------------------------------------------------
        self.log.info("{}: ADDING ADDITIONAL SET (SUBSET OF ALREADY EXISTING "
                      "ONE)".format(current_func_name))

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]

        targets = sorted([
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type],
            ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
        ])
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        vari_requests = [[]]
        perm_requests = [0]

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        expected_result = [TargetProperties(targets=targets, appid=appid)]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[]])
        self.assertListEqual(perm_requests, [0])

        # --------------------------------------------------------------------
        # check that socket_id is added (no vari_requests)
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK THAT SOCKET_ID IS ADDED (no_vari_requests)"
                      .format(current_func_name))

        #            [[<host>, <prio>, <suffix>], ...]
        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        registered_ids = []
        vari_requests = None
        perm_requests = []

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        expected_result = [TargetProperties(targets=targets, appid=appid)]

        self.assertListEqual(registered_ids, expected_result)
        self.assertIsNone(vari_requests)
        self.assertListEqual(perm_requests, [0])

        # --------------------------------------------------------------------
        # check that socket_id is added (no perm_requests)
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK THAT SOCKET_ID IS ADDED (no perm_requests)"
                      .format(current_func_name))

        #            [[<host>, <prio>, <suffix>], ...]
        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        registered_ids = []
        vari_requests = []
        perm_requests = None

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        expected_result = [TargetProperties(targets=targets, appid=appid)]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[]])
        self.assertIsNone(perm_requests)

        # --------------------------------------------------------------------
        # same socke_id but different appid
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK THAT SAME SOCKET_ID IS NOT ADDED TWICE"
                      .format(current_func_name))

        appid2 = 9876543210

        #            [[<host>, <prio>, <suffix>], ...]
        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        # there have to be entries in these two lists as well because len of
        # registered_ids, vari_requests and perm_requests should be the same
        vari_requests = [[]]
        perm_requests = [0]

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            appid=appid2,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        expected_result = [
            TargetProperties(targets=targets, appid=appid),
            TargetProperties(targets=targets, appid=appid2)
        ]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[], []])
        self.assertListEqual(perm_requests, [0, 0])

    def test__stop_signal(self):
        current_func_name = inspect.currentframe().f_code.co_name

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.send_response = mock.MagicMock()
        sighandler.control_pub_socket = mock.MagicMock(
            spec_set=zmq.sugar.socket.Socket
        )

        signal = b"START_STREAM"
        host = self.con_ip
        port = 1234
        send_type = "metadata"
        appid = 1234567890

        mocked_func = sighandler.send_response
        mocked_socket = sighandler.control_pub_socket.send_multipart

        # --------------------------------------------------------------------
        # socket_id not registered
        # --------------------------------------------------------------------
        self.log.info("{}: SOCKET_ID NOT REGISTERED"
                      .format(current_func_name))

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        registered_ids = []
        vari_requests = []
        perm_requests = []

        ret_val = sighandler._stop_signal(
            signal=signal,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )
        new_registered_ids, new_vari_requests, new_perm_requests = ret_val

        expected_args = [b"NO_OPEN_CONNECTION_FOUND"]
        mocked_func.assert_called_once_with(expected_args)

        self.assertListEqual(new_registered_ids, [])
        self.assertListEqual(new_vari_requests, [])
        self.assertListEqual(new_perm_requests, [])

        mocked_func.reset_mock()
        mocked_socket.reset_mock()

        # --------------------------------------------------------------------
        # unregister socket_id (registered for querying data)
        # --------------------------------------------------------------------
        self.log.info("{}: UNREGISTER SOCKET_ID (QUERY)"
                      .format(current_func_name))

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        vari_requests = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]
        perm_requests = None

        ret_val = sighandler._stop_signal(
            signal=signal,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )
        new_registered_ids, new_vari_requests, new_perm_requests = ret_val

        self.assertListEqual(new_registered_ids, [])
        self.assertListEqual(new_vari_requests, [])
        self.assertIsNone(new_perm_requests)

        mocked_func.reset_mock()
        mocked_socket.reset_mock()

        # --------------------------------------------------------------------
        # unregister socket_id (registered for streaming data)
        # --------------------------------------------------------------------
        self.log.info("{}: UNREGISTER SOCKET_ID (STREAM)"
                      .format(current_func_name))

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        vari_requests = None
        perm_requests = [0]

        ret_val = sighandler._stop_signal(
            signal=signal,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )
        new_registered_ids, new_vari_requests, new_perm_requests = ret_val

        self.assertListEqual(new_registered_ids, [])
        self.assertIsNone(new_vari_requests)
        self.assertListEqual(new_perm_requests, [])

        mocked_func.reset_mock()
        mocked_socket.reset_mock()

        # --------------------------------------------------------------------
        # unregister one socket_id from set (open query request)
        # --------------------------------------------------------------------
        self.log.info("{}: UNREGISTER ONE SOCKET_ID FROM SET (NO OPEN QUERY)"
                      .format(current_func_name))

        port2 = 9876

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type],
            ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        vari_requests = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]
        perm_requests = None

        ret_val = sighandler._stop_signal(
            signal=signal,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )
        new_registered_ids, new_vari_requests, new_perm_requests = ret_val

        targets = [
            ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
        ]
        expected_result = [TargetProperties(targets=targets, appid=appid)]
        self.assertListEqual(new_registered_ids, expected_result)
        self.assertListEqual(new_vari_requests, [[]])
        self.assertIsNone(new_perm_requests)

        mocked_func.reset_mock()
        mocked_socket.reset_mock()

        # --------------------------------------------------------------------
        # unregister one socket_id from set (no open query request)
        # --------------------------------------------------------------------
        self.log.info("{}: UNREGISTER ONE SOCKET_ID FROM SET (OPEN QUERY)"
                      .format(current_func_name))

        port2 = 9876

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type],
            ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        vari_requests = [
            [["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]]
        ]
        perm_requests = None

        ret_val = sighandler._stop_signal(
            signal=signal,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )
        new_registered_ids, new_vari_requests, new_perm_requests = ret_val

        targets = [
            ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
        ]
        expected_result = [TargetProperties(targets=targets, appid=appid)]
        self.assertListEqual(new_registered_ids, expected_result)
        self.assertListEqual(new_vari_requests, [targets])
        self.assertIsNone(new_perm_requests)

        mocked_func.reset_mock()
        mocked_socket.reset_mock()

        # --------------------------------------------------------------------
        # unregister one socket_id from set (registered for streaming data)
        # --------------------------------------------------------------------
        self.log.info("{}: UNREGISTER ONE SOCKET_ID FROM SET (STREAM)"
                      .format(current_func_name))

        port2 = 9876

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type],
            ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        vari_requests = None
        perm_requests = [1]

        ret_val = sighandler._stop_signal(
            signal=signal,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )
        new_registered_ids, new_vari_requests, new_perm_requests = ret_val

        targets = [
            ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
        ]
        expected_result = [TargetProperties(targets=targets, appid=appid)]
        self.assertListEqual(new_registered_ids, expected_result)
        self.assertIsNone(new_vari_requests)
        self.assertListEqual(new_perm_requests, [0])

        mocked_func.reset_mock()
        mocked_socket.reset_mock()

        # --------------------------------------------------------------------
        # check signal answering
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK SIGNAL ANSWERING".format(current_func_name))

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        targets = [
            ["{}:{}".format(host, port), 0, re.compile(".*"), send_type]
        ]
        registered_ids = [TargetProperties(targets=targets, appid=appid)]
        vari_requests = None
        perm_requests = None

        ret_val = sighandler._stop_signal(
            signal=signal,
            appid=appid,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )
        new_registered_ids, new_vari_requests, new_perm_requests = ret_val

        self.assertListEqual(new_registered_ids, [])
        self.assertIsNone(new_vari_requests)
        self.assertIsNone(new_perm_requests)

        expected_args = [b"signal",
                         b"CLOSE_SOCKETS",
                         json.dumps(socket_ids).encode("utf-8")]

        mocked_socket.assert_called_once_with(expected_args)

    def test_react_to_signal(self):
        current_func_name = inspect.currentframe().f_code.co_name

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.send_response = mock.MagicMock()
        sighandler._start_signal = mock.MagicMock()
        sighandler._stop_signal = mock.MagicMock()

        unpacked_message_dict = dict(
            check_successful=True,
            response=None,
            appid=None,
            signal=None,
            targets=None
        )

        # --------------------------------------------------------------------
        # check GET_VERSION
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK GET_VERSION".format(current_func_name))

        signal = b"GET_VERSION"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)

        sighandler.react_to_signal(unpacked_message)

        expected_args = [signal, __version__]
        sighandler.send_response.assert_called_once_with(expected_args)

        sighandler.send_response.reset_mock()
        sighandler._start_signal.reset_mock()
        sighandler._stop_signal.reset_mock()

        # --------------------------------------------------------------------
        # check START_STREAM
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK START_STREAM".format(current_func_name))

        signal = b"START_STREAM"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)

        sighandler.react_to_signal(unpacked_message)

        expected_kwargs = {
            "signal": signal,
            "send_type": "data",
            "appid": None,
            "socket_ids": None,
            "registered_ids": [],
            "vari_requests": None,
            "perm_requests": []
        }

        sighandler._start_signal.assert_called_once_with(**expected_kwargs)

        sighandler.send_response.reset_mock()
        sighandler._start_signal.reset_mock()
        sighandler._stop_signal.reset_mock()

        # --------------------------------------------------------------------
        # check START_STREAM_METADATA (storing disabled)
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK START_STREAM_METADATA (STORING DISABLED)"
                      .format(current_func_name))

        signal = b"START_STREAM_METADATA"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)

        # copying the dictionary does not work because log_queue and context
        # should be unique
        config = {}
        for key, value in iteritems(self.signalhandler_config):
            config[key] = value

        config["config"]["store_data"] = False

        # resetting QueueHandlers
        sighandler.log.handlers = []

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**config)

        sighandler.send_response = mock.MagicMock()
        sighandler._start_signal = mock.MagicMock()
        sighandler._stop_signal = mock.MagicMock()

        sighandler.react_to_signal(unpacked_message)

        expected_args = [b"STORING_DISABLED", __version__]
        sighandler.send_response.assert_called_once_with(expected_args)

        sighandler.send_response.reset_mock()
        sighandler._start_signal.reset_mock()
        sighandler._stop_signal.reset_mock()

        # --------------------------------------------------------------------
        # check START_STREAM_METADATA (storing enabled)
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK START_STREAM_METADATA (STORING ENABLED)"
                      .format(current_func_name))

        signal = b"START_STREAM_METADATA"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)

        # copying the dictionary does not work because log_queue and context
        # should be unique
        config = {}
        for key, value in iteritems(self.signalhandler_config):
            config[key] = value

        config["config"]["store_data"] = True

        # resetting QueueHandlers
        sighandler.log.handlers = []

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**config)

        sighandler.send_response = mock.MagicMock()
        sighandler._start_signal = mock.MagicMock()
        sighandler._stop_signal = mock.MagicMock()

        sighandler.react_to_signal(unpacked_message)

        expected_kwargs = {
            "signal": signal,
            "send_type": "metadata",
            "appid": None,
            "socket_ids": None,
            "registered_ids": [],
            "vari_requests": None,
            "perm_requests": []
        }

        sighandler._start_signal.assert_called_once_with(**expected_kwargs)

        sighandler.send_response.reset_mock()
        sighandler._start_signal.reset_mock()
        sighandler._stop_signal.reset_mock()

        # --------------------------------------------------------------------
        # check STOP_STREAM/STOP_STREAM_METADATA
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK STOP_STREAM/STOP_STREAM_METADATA"
                      .format(current_func_name))

        signal = b"STOP_STREAM"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)
        sighandler._stop_signal.return_value = ([], [], [])

        sighandler.react_to_signal(unpacked_message)

        expected_kwargs = {
            "signal": signal,
            "appid": None,
            "socket_ids": None,
            "registered_ids": [],
            "vari_requests": None,
            "perm_requests": []
        }

        sighandler._stop_signal.assert_called_once_with(**expected_kwargs)

        sighandler.send_response.reset_mock()
        sighandler._start_signal.reset_mock()
        sighandler._stop_signal.reset_mock()

        # --------------------------------------------------------------------
        # check START_QUERY_NEXT
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK START_QUERY".format(current_func_name))

        signal = b"START_QUERY_NEXT"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)

        sighandler.react_to_signal(unpacked_message)

        expected_kwargs = {
            "signal": signal,
            "send_type": "data",
            "appid": None,
            "socket_ids": None,
            "registered_ids": [],
            "vari_requests": [],
            "perm_requests": None
        }

        sighandler._start_signal.assert_called_once_with(**expected_kwargs)

        sighandler.send_response.reset_mock()
        sighandler._start_signal.reset_mock()
        sighandler._stop_signal.reset_mock()

        # --------------------------------------------------------------------
        # check START_QUERY_NEXT_METADATA (storing disabled)
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK START_QUERY_NEXT_METADATA (STORING DISABLED)"
                      .format(current_func_name))

        signal = b"START_QUERY_NEXT_METADATA"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)

        # copying the dictionary does not work because log_queue and context
        # should be unique
        config = {}
        for key, value in iteritems(self.signalhandler_config):
            config[key] = value

        config["config"]["store_data"] = False

        # resetting QueueHandlers
        sighandler.log.handlers = []

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**config)

        sighandler.send_response = mock.MagicMock()
        sighandler._start_signal = mock.MagicMock()
        sighandler._stop_signal = mock.MagicMock()

        sighandler.react_to_signal(unpacked_message)

        expected_args = [b"STORING_DISABLED", __version__]
        sighandler.send_response.assert_called_once_with(expected_args)

        sighandler.send_response.reset_mock()
        sighandler._start_signal.reset_mock()
        sighandler._stop_signal.reset_mock()

        # --------------------------------------------------------------------
        # check START_QUERY_NEXT_METADATA (storing enabled)
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK START_QUERY_NEXT_METADATA (STORING ENABLED)"
                      .format(current_func_name))

        signal = b"START_QUERY_NEXT_METADATA"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)

        # copying the dictionary does not work because log_queue and context
        # should be unique
        config = {}
        for key, value in iteritems(self.signalhandler_config):
            config[key] = value

        config["config"]["store_data"] = True

        # resetting QueueHandlers
        sighandler.log.handlers = []

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**config)

        sighandler.send_response = mock.MagicMock()
        sighandler._start_signal = mock.MagicMock()
        sighandler._stop_signal = mock.MagicMock()

        sighandler.react_to_signal(unpacked_message)

        expected_kwargs = {
            "signal": signal,
            "send_type": "metadata",
            "appid": None,
            "socket_ids": None,
            "registered_ids": [],
            "vari_requests": [],
            "perm_requests": None
        }

        sighandler._start_signal.assert_called_once_with(**expected_kwargs)

        sighandler.send_response.reset_mock()
        sighandler._start_signal.reset_mock()
        sighandler._stop_signal.reset_mock()

        # --------------------------------------------------------------------
        # check STOP_QUERY_NEXT/STOP_QUERY_NEXT_METADATA
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK STOP_QUERY_NEXT/STOP_QUERY_NEXT_METADATA"
                      .format(current_func_name))

        signal = b"STOP_QUERY_NEXT"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)
        sighandler._stop_signal.return_value = ([], [], [])

        sighandler.react_to_signal(unpacked_message)

        expected_kwargs = {
            "signal": signal,
            "appid": None,
            "socket_ids": None,
            "registered_ids": [],
            "vari_requests": [],
            "perm_requests": None
        }

        sighandler._stop_signal.assert_called_once_with(**expected_kwargs)

        sighandler.send_response.reset_mock()
        sighandler._start_signal.reset_mock()
        sighandler._stop_signal.reset_mock()

        # --------------------------------------------------------------------
        # check NO_VALID_SIGNAL
        # --------------------------------------------------------------------
        self.log.info("{}: CHECK NO_VALID_SIGNAL".format(current_func_name))

        signal = b"SOME_WEIRD_SIGNAL"
        unpacked_message_dict["signal"] = signal
        unpacked_message = UnpackedMessage(**unpacked_message_dict)

        sighandler.react_to_signal(unpacked_message)

        expected_args = [b"NO_VALID_SIGNAL"]
        sighandler.send_response.assert_called_once_with(expected_args)

    def test_stop(self):
        current_func_name = inspect.currentframe().f_code.co_name

        # --------------------------------------------------------------------
        # external context
        # --------------------------------------------------------------------
        self.log.info("{}: EXTERNAL CONTEXT".format(current_func_name))

        self.signalhandler_config["context"] = self.context

        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)
        sighandler.stop_socket = mock.MagicMock()

        sighandler.stop()

        calls = [
            mock.call(name="com_socket"),
            mock.call(name="request_socket"),
            mock.call(name="request_fw_socket"),
            mock.call(name="control_pub_socket"),
            mock.call(name="control_sub_socket")
        ]
        sighandler.stop_socket.assert_has_calls(calls, any_order=True)

        self.assertIsNotNone(sighandler.context)

        # resetting QueueHandlers
        sighandler.log.handlers = []

        # --------------------------------------------------------------------
        # no external context
        # --------------------------------------------------------------------
        self.log.info("{}: NO EXTERNAL CONTEXT".format(current_func_name))

        self.signalhandler_config["context"] = None
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)
        sighandler.stop_socket = mock.MagicMock()

        sighandler.stop()

        calls = [
            mock.call(name="com_socket"),
            mock.call(name="request_socket"),
            mock.call(name="request_fw_socket"),
            mock.call(name="control_pub_socket"),
            mock.call(name="control_sub_socket")
        ]
        sighandler.stop_socket.assert_has_calls(calls, any_order=True)

        self.assertIsNone(sighandler.context)

    def tearDown(self):
        self.context.destroy(0)

        super(TestSignalHandler, self).tearDown()
