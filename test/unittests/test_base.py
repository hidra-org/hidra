"""Providing a base for all test classes.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import logging
import os
import socket
import tempfile
import unittest
import zmq
from collections import namedtuple
from multiprocessing import Queue
from logutils.queue import QueueHandler

import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

# LOGLEVEL = "error"
LOGLEVEL = "debug"


def create_dir(directory, chmod=None, log=logging):
    """Creates the directory if it does not exist.

    Args:
        directory: The absolute path of the directory to be created.
        chmod (optional): Mode bits to change the permissions of the directory
                          to.
    """

    if not os.path.isdir(directory):
        os.mkdir(directory)
        log.info("Creating directory: {}".format(directory))

    if chmod is not None:
        # the permission have to changed explicitly because
        # on some platform they are ignored when called within mkdir
        os.chmod(directory, 0o777)


ConStr = namedtuple(
    "con_str", [
        "control_pub_bind",
        "control_pub_con",
        "control_sub_bind",
        "control_sub_con",
        "request_fw_bind",
        "request_fw_con",
        "router_bind",
        "router_con",
        "cleaner_job_bind",
        "cleaner_job_con",
        "cleaner_trigger_bind",
        "cleaner_trigger_con",
        "confirm_bind",
        "confirm_con"
    ]
)


def set_con_strs(ext_ip, con_ip, ipc_dir, main_pid, ports):
    """Sets the connection strings.

    Sets the connection strings  for the job, control, trigger and
    confirmation socket.

    Args:
        ext_ip: IP to bind TCP connections to
        con_ip: IP to connect TCP connections to
        ipc_dir: Directory used for IPC connections
        main_pid: Process ID of the current process. Used to distinguish
                  different IPC connection.
        port: A dictionary giving the ports to open TCP connection on
              (only used on Windows).
    Returns:
        A namedtuple object ConStr with the entries:
            control_pub_bind
            control_pub_con
            control_sub_bind
            control_sub_con
            request_fw_bind,
            request_fw_con,
            router_bind,
            router_con,
            cleaner_job_bind
            cleaner_job_con
            cleaner_trigger_bind
            cleaner_trigger_con
            confirm_bind
            confirm_con
    """

    # determine socket connection strings
    if utils.is_windows():
        control_pub_bind_str = "tcp://{}:{}".format(ext_ip, ports["control_pub"])
        control_pub_con_str = "tcp://{}:{}".format(con_ip, ports["control_pub"])

        control_sub_bind_str = "tcp://{}:{}".format(ext_ip, ports["control_sub"])
        control_sub_con_str = "tcp://{}:{}".format(con_ip, ports["control_sub"])

        request_fw_bind_str = "tcp://{}:{}".format(ext_ip, ports["request_fw"])
        request_fw_con_str = "tcp://{}:{}".format(con_ip, ports["request_fw"])

        router_bind_str = "tcp://{}:{}".format(ext_ip, ports["router"])
        router_con_str = "tcp://{}:{}".format(con_ip, ports["router"])

        job_bind_str = "tcp://{}:{}".format(ext_ip, ports["cleaner"])
        job_con_str = "tcp://{}:{}".format(con_ip, ports["cleaner"])

        trigger_bind_str = "tcp://{}:{}".format(ext_ip,
                                                ports["cleaner_trigger"])
        trigger_con_str = "tcp://{}:{}".format(con_ip,
                                               ports["cleaner_trigger"])
    else:
        ipc_ip = "{}/{}".format(ipc_dir, main_pid)

        control_pub_bind_str = "ipc://{}_{}".format(ipc_ip, "control_pub")
        control_pub_con_str = control_pub_bind_str

        control_sub_bind_str = "ipc://{}_{}".format(ipc_ip, "control_sub")
        control_sub_con_str = control_sub_bind_str

        request_fw_bind_str = "ipc://{}_{}".format(ipc_ip, "request_fw")
        request_fw_con_str = request_fw_bind_str

        router_bind_str = "ipc://{}_{}".format(ipc_ip, "router")
        router_con_str = router_bind_str

        job_bind_str = "ipc://{}_{}".format(ipc_ip, "cleaner")
        job_con_str = job_bind_str

        trigger_bind_str = "ipc://{}_{}".format(ipc_ip, "cleaner_trigger")
        trigger_con_str = trigger_bind_str

    confirm_con_str = "tcp://{}:{}".format(con_ip, ports["confirmation"])
    confirm_bind_str = "tcp://{}:{}".format(ext_ip, ports["confirmation"])

    return ConStr(
        control_pub_bind=control_pub_bind_str,
        control_pub_con=control_pub_con_str,
        control_sub_bind=control_sub_bind_str,
        control_sub_con=control_sub_con_str,
        request_fw_bind=request_fw_bind_str,
        request_fw_con=request_fw_con_str,
        router_bind=router_bind_str,
        router_con=router_con_str,
        cleaner_job_bind=job_bind_str,
        cleaner_job_con=job_con_str,
        cleaner_trigger_bind=trigger_bind_str,
        cleaner_trigger_con=trigger_con_str,
        confirm_bind=confirm_bind_str,
        confirm_con=confirm_con_str
    )


class TestBase(unittest.TestCase):
    """The Base class from which all data fetchers should inherit from.
    """

    def setUp(self):
        global LOGLEVEL

        self.log_queue = False
        self.listener = None
        self.log = None

        main_pid = os.getpid()
        self.con_ip = socket.getfqdn()
        self.ext_ip = socket.gethostbyaddr(self.con_ip)[2][0]
        ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")

        ports = {
            "control_pub": 50005,
            "control_sub": 50006,
            "request_fw": 6001,
            "router": 7000,
            "cleaner": 50051,
            "cleaner_trigger": 50052,
            "confirmation": 50053,
        }

        con_strs = set_con_strs(ext_ip=self.ext_ip,
                                con_ip=self.con_ip,
                                ipc_dir=ipc_dir,
                                main_pid=main_pid,
                                ports=ports)

        self.config = {
            "ports": ports,
            "ipc_dir": ipc_dir,
            "main_pid": main_pid,
            "con_strs": con_strs,
        }

        self._init_logging(loglevel=LOGLEVEL)

#        self.log.debug("{} pid {}".format(self.__class__.__name__, main_pid))

    def _init_logging(self, loglevel="debug"):
        """Initialize log listener and log queue.

        Args:
            loglevel: The log level with of StreamHandler to be started.
        """

        loglevel = loglevel.lower()

        # Create handler
        handler = utils.get_stream_log_handler(loglevel=loglevel)

        # Start queue listener using the stream handler above
        self.log_queue = Queue(-1)
        self.listener = utils.CustomQueueListener(self.log_queue, handler)
        self.listener.start()

        # Create log and set handler to queue handle
        root = logging.getLogger()
        qhandler = QueueHandler(self.log_queue)
        root.addHandler(qhandler)

        self.log = utils.get_logger("test_datafetcher", self.log_queue)

    def set_up_recv_socket(self, port):
        """Create pull socket and connect to port.

        Args:
            port: Port to connect to.
        """

        sckt = self.context.socket(zmq.PULL)
        con_str = "tcp://{}:{}".format(self.ext_ip, port)
        sckt.bind(con_str)
        self.log.info("Start receiving socket (bind): {}".format(con_str))

        return sckt

    def tearDown(self):
        if self.listener is not None:
            self.log_queue.put_nowait(None)
            self.listener.stop()
            self.listener = None
