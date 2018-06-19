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
# LOGLEVEL = "info"
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


IpcEndpoints = namedtuple(
    "ipc_endpoints", [
        "control_pub",
        "control_sub",
        "request_fw",
        "router",
        "cleaner_job",
        "cleaner_trigger",
    ]
)


def set_ipc_endpoints(ipc_dir, main_pid):
    """Sets the ipc connection paths.

    Sets the connection strings  for the job, control, trigger and
    confirmation socket.

    Args:
        ipc_dir: Directory used for IPC connections
        main_pid: Process ID of the current process. Used to distinguish
                  different IPC connection.
    Returns:
        A namedtuple object ConStr with the entries:
            control_pub_path,
            control_sub_path,
            request_fw_path,
            router_path,
            cleaner_job_path,
            cleaner_trigger_path
    """

    # determine socket connection strings
    ipc_ip = "{}/{}".format(ipc_dir, main_pid)

    control_pub_path = "{}_{}".format(ipc_ip, "control_pub")
    control_sub_path = "{}_{}".format(ipc_ip, "control_sub")
    request_fw_path = "{}_{}".format(ipc_ip, "request_fw")
    router_path = "{}_{}".format(ipc_ip, "router")
    job_path = "{}_{}".format(ipc_ip, "cleaner")
    trigger_path = "{}_{}".format(ipc_ip, "cleaner_trigger")

    return IpcEndpoints(
        control_pub=control_pub_path,
        control_sub=control_sub_path,
        request_fw=request_fw_path,
        router=router_path,
        cleaner_job=job_path,
        cleaner_trigger=trigger_path,
    )


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
        "confirm_con",
        "com_bind",
        "com_con",
    ]
)


def set_con_strs(ext_ip, con_ip, ipc_endpoints, ports):
    """Sets the connection strings.

    Sets the connection strings  for the job, control, trigger and
    confirmation socket.

    Args:
        ext_ip: IP to bind TCP connections to
        con_ip: IP to connect TCP connections to
        ipc_endpoints: Endpoints to use for the IPC connections.
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
            com_bind
            com_con
    """

    # determine socket connection strings
    if utils.is_windows():
        port = ports["control_pub"]
        control_pub_bind_str = "tcp://{}:{}".format(ext_ip, port)
        control_pub_con_str = "tcp://{}:{}".format(con_ip, port)

        port = ports["control_sub"]
        control_sub_bind_str = "tcp://{}:{}".format(ext_ip, port)
        control_sub_con_str = "tcp://{}:{}".format(con_ip, port)

        port = ports["request_fw"]
        request_fw_bind_str = "tcp://{}:{}".format(ext_ip, port)
        request_fw_con_str = "tcp://{}:{}".format(con_ip, port)

        port = ports["router"]
        router_bind_str = "tcp://{}:{}".format(ext_ip, port)
        router_con_str = "tcp://{}:{}".format(con_ip, port)

        port = ports["cleaner"]
        job_bind_str = "tcp://{}:{}".format(ext_ip, port)
        job_con_str = "tcp://{}:{}".format(con_ip, port)

        port = ports["cleaner_trigger"]
        trigger_bind_str = "tcp://{}:{}".format(ext_ip, port)
        trigger_con_str = "tcp://{}:{}".format(con_ip, port)
    else:
        control_pub_bind_str = "ipc://{}".format(ipc_endpoints.control_pub)
        control_pub_con_str = control_pub_bind_str

        control_sub_bind_str = "ipc://{}".format(ipc_endpoints.control_sub)
        control_sub_con_str = control_sub_bind_str

        request_fw_bind_str = "ipc://{}".format(ipc_endpoints.request_fw)
        request_fw_con_str = request_fw_bind_str

        router_bind_str = "ipc://{}".format(ipc_endpoints.router)
        router_con_str = router_bind_str

        job_bind_str = "ipc://{}".format(ipc_endpoints.cleaner_job)
        job_con_str = job_bind_str

        trigger_bind_str = "ipc://{}".format(ipc_endpoints.cleaner_trigger)
        trigger_con_str = trigger_bind_str

    confirm_bind_str = "tcp://{}:{}".format(ext_ip, ports["confirmation"])
    confirm_con_str = "tcp://{}:{}".format(con_ip, ports["confirmation"])

    com_bind_str = "tcp://{}:{}".format(ext_ip, ports["com"])
    com_con_str = "tcp://{}:{}".format(con_ip, ports["com"])

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
        confirm_con=confirm_con_str,
        com_bind=com_bind_str,
        com_con=com_con_str
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
            "com": 50000,
            "request_fw": 50002,
            "control_pub": 50005,
            "control_sub": 50006,
            "router": 50004,
            "cleaner": 50051,
            "cleaner_trigger": 50052,
            "confirmation": 50053,
        }

        self.ipc_endpoints = set_ipc_endpoints(ipc_dir=ipc_dir,
                                               main_pid=main_pid)

        con_strs = set_con_strs(ext_ip=self.ext_ip,
                                con_ip=self.con_ip,
                                ipc_endpoints=self.ipc_endpoints,
                                ports=ports)

        self.config = {
            "ports": ports,
            "ipc_dir": ipc_dir,
            "main_pid": main_pid,
            "con_strs": con_strs,
        }

        self._init_logging(loglevel=LOGLEVEL)

#        self.log.debug("{} pid {}".format(self.__class__.__name__, main_pid))

    def __iter__(self):
        for attr, value in self.__dict__.iteritems():
            yield attr, value

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

        con_str = "tcp://{}:{}".format(self.ext_ip, port)
        try:
            sckt = self.context.socket(zmq.PULL)
            sckt.bind(con_str)
            self.log.info("Start receiving socket (bind): {}".format(con_str))
        except:
            self.log.error("Failed to start receiving socket (bind): {}"
                           .format(con_str))
            raise

        return sckt

    def tearDown(self):
        for key, endpoint in vars(self.ipc_endpoints).iteritems():
            try:
                os.remove(endpoint)
                self.log.debug("Removed ipc socket: {}".format(endpoint))
            except OSError:
                self.log.debug("Could not remove ipc socket: {}"
                               .format(endpoint))
            except:
                self.log.warning("Could not remove ipc socket: {}"
                                 .format(endpoint), exc_info=True)

        if self.listener is not None:
            self.log_queue.put_nowait(None)
            self.listener.stop()
            self.listener = None
