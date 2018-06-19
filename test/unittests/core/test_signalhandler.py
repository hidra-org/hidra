"""Testing the task provider.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import threading
import time
import zmq
from multiprocessing import freeze_support

import utils
from .__init__ import BASE_DIR
from test_base import TestBase, create_dir
from signalhandler import SignalHandler
from _version import __version__

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class RequestPuller(threading.Thread):
    def __init__(self, con_strs, log_queue):
        threading.Thread.__init__(self)

        self.log = utils.get_logger("RequestPuller", log_queue)
        self.continue_run = True

        self.context = zmq.Context()
        self.request_fw_socket = self.context.socket(zmq.REQ)
        self.request_fw_socket.connect(con_strs.request_fw_con)
        self.log.info("request_fw_socket started (connect) for '{}'"
                      .format(con_strs.request_fw_con))

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

        self.signalhandler_config = {
            "ldapuri": "it-ldap-slave.desy.de:1389",
            "store_data": False
        }

        self.receiving_ports = [6005, 6006]

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

    def test_signalhandler(self):
        """Simulate incoming data and check if received events are correct.
        """

        whitelist = ["localhost", self.con_ip]

        ports = {
            "com": 6000,
            "request": 6002
        }

        con_strs = self.config["con_strs"]

        com_con_str = "tcp://{}:{}".format(self.ext_ip, ports["com"])
        request_con_str = "tcp://{}:{}".format(self.ext_ip, ports["request"])

        receiving_con_strs = []
        for port in self.receiving_ports:
            receiving_con_strs.append("{}:{}".format(self.con_ip, port))

        # create control socket
        # control messages are not send over an forwarder, thus the
        # control_sub endpoint is used directly
        control_pub_socket = self.context.socket(zmq.PUB)
        control_pub_socket.bind(con_strs.control_sub_bind)
        self.log.info("control_pub_socket connect to: '{}'"
                      .format(con_strs.control_sub_bind))

        kwargs = dict(
            params=self.signalhandler_config,
            control_pub_con_id=con_strs.control_pub_con,
            control_sub_con_id=con_strs.control_sub_con,
            whitelist=whitelist,
            ldapuri=self.signalhandler_config["ldapuri"],
            com_con_id=com_con_str,
            request_fw_con_id=con_strs.request_fw_bind,
            request_con_id=request_con_str,
            log_queue=self.log_queue,
        )
        signalhandler_thr = threading.Thread(target=SignalHandler,
                                             kwargs=kwargs)
        signalhandler_thr.start()

        # to give the signal handler to bind to the socket before the connect
        # is done
        time.sleep(0.5)

        request_puller_thr = RequestPuller(con_strs,
                                           self.log_queue)
        request_puller_thr.start()

        com_socket = self.context.socket(zmq.REQ)
        com_socket.connect(com_con_str)
        self.log.info("com_socket connected to {}".format(com_con_str))

        request_socket = self.context.socket(zmq.PUSH)
        request_socket.connect(request_con_str)
        self.log.info("request_socket connected to {}".format(request_con_str))

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
                              socket_id=receiving_con_strs[1])

            self.send_signal(socket=com_socket,
                             signal=b"START_QUERY_NEXT",
                             ports=self.receiving_ports,
                             prio=2)

            self.send_request(socket=request_socket,
                              socket_id=receiving_con_strs[1].encode())
            self.send_request(socket=request_socket,
                              socket_id=receiving_con_strs[1].encode())
            self.send_request(socket=request_socket,
                              socket_id=receiving_con_strs[0].encode())

            self.cancel_request(socket=request_socket,
                                socket_id=receiving_con_strs[1].encode())

            time.sleep(0.5)

            self.send_request(socket=request_socket,
                              socket_id=receiving_con_strs[0])
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

            control_pub_socket.close(0)

            com_socket.close(0)
            request_socket.close(0)

    def tearDown(self):
        self.context.destroy(0)

        super(TestSignalHandler, self).tearDown()
