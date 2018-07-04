"""Testing the task provider.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import copy
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
from signalhandler import SignalHandler
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

        send_message = [__version__, signal]

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

        whitelist = ["localhost", self.con_ip]

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

        #---------------------------------------------------------------------
        # external context
        #---------------------------------------------------------------------
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            sighandler.setup(**setup_conf)

        self.assertIsInstance(sighandler.log, logging.Logger)
        self.assertEqual(sighandler.whitelist, conf["whitelist"])
        self.assertEqual(sighandler.context, conf["context"])
        self.assertTrue(sighandler.ext_context)

        # resetting QueueHandlers
        sighandler.log.handlers = []

        #---------------------------------------------------------------------
        # no external context
        #---------------------------------------------------------------------
        setup_conf["context"] = None
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            sighandler.setup(**setup_conf)

        self.assertIsInstance(sighandler.log, logging.Logger)
        self.assertEqual(sighandler.whitelist, conf["whitelist"])
        self.assertIsInstance(sighandler.context, zmq.Context)
        self.assertFalse(sighandler.ext_context)

    @mock.patch("signalhandler.SignalHandler.stop")
    def test_create_sockets(self, mock_stop):

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

        #---------------------------------------------------------------------
        # with no nodes allowed to connect
        #---------------------------------------------------------------------
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

        #---------------------------------------------------------------------
        # with all nodes allowed to connect
        #---------------------------------------------------------------------
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

    def todo_test_run(self):
        pass

    def test_check_signal_inverted(self):
        # with all nodes allowed to connect
        self.signalhandler_config["whitelist"] = None
        with mock.patch("signalhandler.SignalHandler.setup"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.log = MockLogging()

        host = self.con_ip
        port = 1234
        in_targets_list = [["{}:{}".format(host, port), 0, [""]]]
        in_targets = json.dumps(in_targets_list).encode("utf-8")

        #---------------------------------------------------------------------
        # no valid message
        #---------------------------------------------------------------------
        in_message = []
        check_failed, signal, target = sighandler.check_signal_inverted(in_message)
        self.assertEqual(check_failed, [b"NO_VALID_SIGNAL"])
        self.assertIsNone(signal)
        self.assertIsNone(target)

        #---------------------------------------------------------------------
        # no valid message due to missing port
        #---------------------------------------------------------------------
        fake_in_targets = [["{}".format(host), 0, [""]]]
        fake_in_targets = json.dumps(fake_in_targets).encode("utf-8")

        in_message = [__version__, "START_STREAM", fake_in_targets]
        check_failed, signal, target = sighandler.check_signal_inverted(in_message)
        self.assertEqual(check_failed, [b"NO_VALID_SIGNAL"])
        self.assertIsNone(signal)
        self.assertIsNone(target)

        #---------------------------------------------------------------------
        # no version set
        #---------------------------------------------------------------------
        in_message = [None, "START_STREAM", in_targets]
        check_failed, signal, target = sighandler.check_signal_inverted(in_message)
        self.assertEqual(check_failed, [b"NO_VALID_SIGNAL"])
        self.assertIsNone(signal)
        self.assertIsNone(target)

        #---------------------------------------------------------------------
        # valid message but version conflict
        #---------------------------------------------------------------------
        version = "0.0.0"
        #             version, signal, targets
        in_message = [version, "START_STREAM", in_targets]
        check_failed, signal, target = sighandler.check_signal_inverted(in_message)
        self.assertEqual(check_failed, ["VERSION_CONFLICT", __version__])
        self.assertIsNone(signal)
        self.assertIsNone(target)

        #---------------------------------------------------------------------
        # no version set (empty string)
        #---------------------------------------------------------------------
        in_message = ["", "START_STREAM", in_targets]
        check_failed, signal, target = sighandler.check_signal_inverted(in_message)
        self.log.debug("check_failed {}".format(check_failed))
        self.assertFalse(check_failed)
        self.assertEqual(signal, "START_STREAM")
        self.assertEqual(target, in_targets_list)

        #---------------------------------------------------------------------
        # valid message, valid version
        #---------------------------------------------------------------------
        #             version, signal, targets
        in_message = [__version__, "START_STREAM", in_targets]
        check_failed, signal, target = sighandler.check_signal_inverted(in_message)
        self.assertFalse(check_failed)
        self.assertEqual(signal, "START_STREAM")
        self.assertEqual(target, in_targets_list)

        # resetting QueueHandlers
        sighandler.log.handlers = []

        # with no nodes allowed to connect
        self.signalhandler_config["whitelist"] = []
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.log = MockLogging()

        #---------------------------------------------------------------------
        # valid message, valid version, but host is not allowed to connect
        #---------------------------------------------------------------------
        #             version, signal, targets
        in_message = [__version__, "START_STREAM", in_targets]
        check_failed, signal, target = sighandler.check_signal_inverted(in_message)
        self.assertEqual(check_failed, [b"NO_VALID_HOST"])
        self.assertIsNone(signal)
        self.assertIsNone(target)

    def test_send_response(self):
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.com_socket = mock.MagicMock(spec_set=zmq.sugar.socket.Socket)

        mocked_func = sighandler.com_socket.send_multipart

        #---------------------------------------------------------------------
        # signal is sring
        #---------------------------------------------------------------------
        signal = "test"
        sighandler.send_response(signal)
        mocked_func.assert_called_once()
        mocked_func.call_args[0] == [signal]

        mocked_func.reset_mock()

        #---------------------------------------------------------------------
        # signal is list
        #---------------------------------------------------------------------
        signal = ["test"]
        sighandler.send_response(signal)
        mocked_func.assert_called_once()
        mocked_func.call_args[0] == signal

    def test__start_signal(self):
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.send_response = mock.MagicMock()

        signal = b"START_STREAM"
        send_type = "metadata"
        host = self.con_ip
        port = 1234

        #---------------------------------------------------------------------
        # check that socket_id is added
        #---------------------------------------------------------------------
        #            [[<host>, <prio>, <suffix>], ...]
        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
        #socket_ids = [["{}:{}".format(host, port), 0, [""]]]
        registered_ids = []
        vari_requests = []
        perm_requests = []

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        expected_result = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[]])
        self.assertListEqual(perm_requests, [0])

        #---------------------------------------------------------------------
        # check that same socke_id is not added twice
        #---------------------------------------------------------------------
        #            [[<host>, <prio>, <suffix>], ...]
        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]

        registered_ids = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]
        # there have to be entries in these two lists as well because len of
        # registered_ids, vari_requests and perm_requests should be the same
        vari_requests = [[]]
        perm_requests = [0]

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        expected_result = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[]])
        self.assertListEqual(perm_requests, [0])

        #---------------------------------------------------------------------
        # adding additional set
        #---------------------------------------------------------------------
        host2 = "abc"
        port2 = 9876
        socket_ids = [["{}:{}".format(host, port), 0, ".*"],
                      ["{}:{}".format(host, port2), 0, ".*"]]

        registered_ids = [
            [["{}:{}".format(host2, port), 0, re.compile(".*"), send_type]]
        ]
        vari_requests = [[]]
        perm_requests = [0]

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        expected_result = [
            [["{}:{}".format(host2, port), 0, re.compile(".*"), send_type]],
            sorted([
                ["{}:{}".format(host, port), 0, re.compile(".*"), send_type],
                ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
            ])
        ]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[], []])
        self.assertListEqual(perm_requests, [0, 0])

        #---------------------------------------------------------------------
        # adding additional set (superset of already existing one)
        #---------------------------------------------------------------------

        socket_ids = [["{}:{}".format(host, port), 0, ".*"],
                      ["{}:{}".format(host, port2), 0, ".*"]]

        registered_ids = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]
        vari_requests = [[]]
        perm_requests = [0]

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        expected_result = [
            sorted([
                ["{}:{}".format(host, port), 0, re.compile(".*"), send_type],
                ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
            ])
        ]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[]])
        self.assertListEqual(perm_requests, [0])

        #---------------------------------------------------------------------
        # adding additional set (subset of already existing one)
        #---------------------------------------------------------------------

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]

        registered_ids = [
            sorted([
                ["{}:{}".format(host, port), 0, re.compile(".*"), send_type],
                ["{}:{}".format(host, port2), 0, re.compile(".*"), send_type]
            ])
        ]
        vari_requests = [[]]
        perm_requests = [0]

        sighandler._start_signal(
            signal=signal,
            send_type=send_type,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )

        expected_result = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]

        self.assertListEqual(registered_ids, expected_result)
        self.assertListEqual(vari_requests, [[]])
        self.assertListEqual(perm_requests, [0])

    def test__stop_signal(self):
        with mock.patch("signalhandler.SignalHandler.create_sockets"):
            with mock.patch("signalhandler.SignalHandler.exec_run"):
                sighandler = SignalHandler(**self.signalhandler_config)

        sighandler.send_response = mock.MagicMock()
        sighandler.control_pub_socket = mock.MagicMock()

        signal = b"START_STREAM"
        host = self.con_ip
        port = 1234
        send_type = "metadata"

        mocked_func = sighandler.send_response

        #---------------------------------------------------------------------
        # socket_id not registered
        #---------------------------------------------------------------------

#        mocked_func.reset_mock()
#
#        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
#        registered_ids = []
#        vari_requests = []
#        perm_requests = []
#
#        ret_val = sighandler._stop_signal(
#            signal=signal,
#            socket_ids=socket_ids,
#            registered_ids=registered_ids,
#            vari_requests=vari_requests,
#            perm_requests=perm_requests
#        )
#        new_registered_ids, new_vari_requests, new_perm_requests = ret_val
#        mocked_func.assert_called_once()
#        mocked_func.call_args[0] == [b"NO_OPEN_CONNECTION_FOUND"]
#
#        self.assertListEqual(new_registered_ids, [])
#        self.assertListEqual(new_vari_requests, [])
#        self.assertListEqual(new_perm_requests, [])

        #---------------------------------------------------------------------
        # unregister socket_id (registered for querying data)
        #---------------------------------------------------------------------

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
#        registered_ids = []
        registered_ids = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]
        vari_requests = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]
        perm_requests = [0]

        ret_val = sighandler._stop_signal(
            signal=signal,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )
        new_registered_ids, new_vari_requests, new_perm_requests = ret_val

        self.assertListEqual(new_registered_ids, [])
        self.assertListEqual(new_vari_requests, [])
        self.assertListEqual(new_perm_requests, [])

        #---------------------------------------------------------------------
        # unregister socket_id (registered for querying data)
        #---------------------------------------------------------------------

        socket_ids = [["{}:{}".format(host, port), 0, ".*"]]
#        registered_ids = []
        registered_ids = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]
        vari_requests = [
            [["{}:{}".format(host, port), 0, re.compile(".*"), send_type]]
        ]
        perm_requests = [0]

        ret_val = sighandler._stop_signal(
            signal=signal,
            socket_ids=socket_ids,
            registered_ids=registered_ids,
            vari_requests=vari_requests,
            perm_requests=perm_requests
        )
        new_registered_ids, new_vari_requests, new_perm_requests = ret_val

        self.assertListEqual(new_registered_ids, [])
        self.assertListEqual(new_vari_requests, [])
        self.assertListEqual(new_perm_requests, [])

    def todo_test_react_to_signal(self):
        pass

    def todo_test_stop(self):
        pass

    def tearDown(self):
        self.context.destroy(0)

        super(TestSignalHandler, self).tearDown()
