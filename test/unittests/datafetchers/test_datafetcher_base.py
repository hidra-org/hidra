"""Providing a base for the data fetchers test classes.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import logging
import os
import unittest
import socket
import tempfile
import zmq
from collections import namedtuple
from multiprocessing import Queue
from logutils.queue import QueueHandler

import utils


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
        "control_bind",
        "control_con",
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
            job_bind
            job_con
            control_bind
            control_con
            trigger_bind
            trigger_con
            confirm_bind
            confirm_con
    """

    # determine socket connection strings
    if utils.is_windows():
        job_bind_str = "tcp://{}:{}".format(ext_ip, ports["cleaner"])
        job_con_str = "tcp://{}:{}".format(con_ip, ports["cleaner"])

        control_bind_str = "tcp://{}:{}".format(ext_ip, ports["control"])
        control_con_str = "tcp://{}:{}".format(con_ip, ports["control"])

        trigger_bind_str = "tcp://{}:".format(ext_ip)
        trigger_bind_str += str(ports["cleaner_trigger"])
        trigger_con_str = "tcp://{}:".format(con_ip)
        trigger_con_str += str(ports["cleaner_trigger"])
    else:
        ipc_ip = "{}/{}".format(ipc_dir, main_pid)

        job_bind_str = "ipc://{}_{}".format(ipc_ip, "cleaner")
        job_con_str = job_bind_str

        control_bind_str = "ipc://{}_{}".format(ipc_ip, "control")
        control_con_str = control_bind_str

        trigger_bind_str = "ipc://{}_{}".format(ipc_ip, "cleaner_trigger")
        trigger_con_str = trigger_bind_str

    confirm_con_str = "tcp://{}:{}".format(con_ip, ports["confirmation_port"])
    confirm_bind_str = "tcp://{}:{}".format(ext_ip, ports["confirmation_port"])

    return ConStr(
        control_bind=control_bind_str,
        control_con=control_con_str,
        cleaner_job_bind=job_bind_str,
        cleaner_job_con=job_con_str,
        cleaner_trigger_bind=trigger_bind_str,
        cleaner_trigger_con=trigger_con_str,
        confirm_bind=confirm_bind_str,
        confirm_con=confirm_con_str
    )


class TestDataFetcherBase(unittest.TestCase):
    """The Base class from which all data fetchers should inherit from.
    """

    def setUp(self):
        self.config = {}
        self.log_queue = False
        self.listener = None
        self.log = None

        self._init_logging()

        main_pid = os.getpid()
        self.con_ip = socket.getfqdn()
        self.ext_ip = socket.gethostbyaddr(self.con_ip)[2][0]
        ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")

        create_dir(directory=ipc_dir, chmod=0o777)

        self.context = zmq.Context.instance()

        ports = {
            "control": 50005,
            "cleaner": 50051,
            "cleaner_trigger": 50052,
            "confirmation_port": 50053,
        }

        con_strs = set_con_strs(ext_ip=self.ext_ip,
                                con_ip=self.con_ip,
                                ipc_dir=ipc_dir,
                                main_pid=main_pid,
                                ports=ports)

        # Set up config
        self.config = {
            "ipc_dir": ipc_dir,
            "main_pid": main_pid,
            "con_strs": con_strs
        }

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

    def _set_up_socket(self, port):
        """Create pull socket and connect to port.

        Args:
            port: Port to connect to.
        """

        sckt = self.context.socket(zmq.PULL)
        connection_str = "tcp://{}:{}".format(self.ext_ip, port)
        sckt.bind(connection_str)
        self.log.info("Start receiving socket (bind): {}"
                      .format(connection_str))

        return sckt

    def tearDown(self):
        if self.listener is not None:
            self.log_queue.put_nowait(None)
            self.listener.stop()
            self.listener = None
