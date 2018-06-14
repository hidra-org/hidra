"""Testing the task provider.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import socket
import tempfile
import threading
import time
import zmq
from multiprocessing import Process, freeze_support
from shutil import copyfile

import utils
from .__init__ import BASE_DIR
from test_base import TestBase, create_dir
from signalhandler import SignalHandler
from _version import __version__

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class RequestPuller():
    def __init__(self, request_fw_con_id, log_queue, context=None):

        self.log = utils.get_logger("RequestPuller", log_queue)

        # to give the signal handler to bind to the socket before the connect
        # is done
        time.sleep(0.5)

        self.context = context or zmq.Context()
        self.request_fw_socket = self.context.socket(zmq.REQ)
        self.request_fw_socket.connect(request_fw_con_id)
        self.log.info("request_fw_socket started (connect) for '{}'"
                      .format(request_fw_con_id))

        self.run()

    def run(self):
        self.log.info("Start run")
        filename = "test_file.cbf"
        while True:
            try:
                self.request_fw_socket.send_multipart(
                    [b"GET_REQUESTS", json.dumps(filename).encode("utf-8")])
                self.log.info("[getRequests] send")
                requests = json.loads(self.request_fw_socket.recv_string())
                self.log.info("Requests: {}".format(requests))
                time.sleep(0.25)
            except Exception as e:
                self.log.error(str(e), exc_info=True)
                break

    def __exit__(self):
        self.request_fw_socket.close(0)
        self.context.destroy()



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
        self.context = zmq.Context.instance()

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

        # initiate forwarder for control signals (multiple pub, multiple sub)
        device = zmq.devices.ThreadDevice(zmq.FORWARDER, zmq.SUB, zmq.PUB)
        device.bind_in(con_strs.control_pub_bind)
        device.bind_out(con_strs.control_sub_bind)
        device.setsockopt_in(zmq.SUBSCRIBE, b"")
        device.start()

        # create control socket
        control_pub_socket = self.context.socket(zmq.PUB)
        control_pub_socket.connect(con_strs.control_pub_con)
        self.log.info("control_pub_socket connect to: '{}'"
                      .format(con_strs.control_pub_con))

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
            context=self.context
        )
        signalhandler_pr = threading.Thread(target=SignalHandler, kwargs=kwargs)
        signalhandler_pr.start()

        request_puller_pr = Process(target=RequestPuller,
                                    args=(
                                        con_strs.request_fw_con,
                                        self.log_queue)
                                    )
        request_puller_pr.start()

        com_socket = self.context.socket(zmq.REQ)
        com_socket.connect(com_con_str)
        self.log.info("com_socket connected to {}".format(com_con_str))

        request_socket = self.context.socket(zmq.PUSH)
        request_socket.connect(request_con_str)
        self.log.info("request_socket connected to {}". format(request_con_str))

        time.sleep(1)

        try:
            self.send_signal(com_socket, b"START_STREAM", 6003, 1)

            self.send_signal(com_socket, b"START_STREAM", 6004, 0)

            self.send_signal(com_socket, b"STOP_STREAM", 6003)

            self.send_request(request_socket, receiving_con_strs[1])

            self.send_signal(com_socket, b"START_QUERY_NEXT", self.receiving_ports, 2)

            self.send_request(request_socket, receiving_con_strs[1].encode())
            self.send_request(request_socket, receiving_con_strs[1].encode())
            self.send_request(request_socket, receiving_con_strs[0].encode())

            self.cancel_request(request_socket, receiving_con_strs[1].encode())

            time.sleep(0.5)

            self.send_request(request_socket, receiving_con_strs[0])
            self.send_signal(com_socket, b"STOP_QUERY_NEXT", self.receiving_ports[0], 2)

            time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:

            control_pub_socket.send_multipart([b"control", b"EXIT"])
            self.log.debug("EXIT")

            signalhandler_pr.join()
            request_puller_pr.terminate()

            control_pub_socket.close(0)

            com_socket.close(0)
            request_socket.close(0)

    def tearDown(self):
        self.context.destroy(0)

#        try:
#            os.remove(control_pub_path)
#            self.log.debug("Removed ipc socket: {}".format(control_pub_path))
#        except OSError:
#            self.log.warning("Could not remove ipc socket: {}"
#                            .format(control_pub_path))
#        except:
#            self.log.warning("Could not remove ipc socket: {}"
#                            .format(control_pub_path), exc_info=True)
#
#        try:
#            os.remove(control_sub_path)
#            self.log.debug("Removed ipc socket: {}".format(control_sub_path))
#        except OSError:
#            self.log.warning("Could not remove ipc socket: {}"
#                             .format(control_sub_path))
#        except:
#            self.log.warning("Could not remove ipc socket: {}"
#                             .format(control_sub_path), exc_info=True)


        super(TestSignalHandler, self).tearDown()
