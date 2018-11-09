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

"""Providing a base for all test classes.
"""

# pylint: disable=global-variable-not-assigned

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import inspect
import logging
from multiprocessing import Queue
import os
import socket as m_socket
import tempfile
import traceback
import unittest
import zmq

from logutils.queue import QueueHandler
import mock

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


class MockLogging(mock.MagicMock, utils.LoggingFunction):
    """ Mock the logging module.
    """

    def __init__(self, **kwargs):
        mock.MagicMock.__init__(self, **kwargs)
        utils.LoggingFunction.__init__(self, level="debug")

    def out(self, message, *args, **kwargs):
        """Forward the output to stdout.

        Args:
            message: the messages to be logged.
            args: The arguments to fill in into msg.
            exc_info (optional): Append a traceback.
        """

        msg = str(message)
        if args:
            msg = msg % args


        caller = inspect.getframeinfo(inspect.stack()[1][0])
        fname = os.path.split(caller.filename)[1]
        msg = "[{}:{}] > {}".format(fname, caller.lineno, msg)

        if "exc_info" in kwargs and kwargs["exc_info"]:
            print(msg, traceback.format_exc())
        else:
            print(msg)


def mock_get_logger(logger_name, queue=False, log_level="debug"):
    """Wrapper for the get_logger function
    """
    # pylint: disable=unused-argument

    return MockLogging()


class MockZmqSocket(mock.MagicMock):
    """Mock a zmq socket.
    """

    def __init__(self, **kwargs):
        super(MockZmqSocket, self).__init__(**kwargs)
        self._connected = False

        self.send_multipart = mock.MagicMock()
        self.recv_multipart = mock.MagicMock()

    def bind(self, endpoint):
        """Mock the socket bind method.
        """
        assert not self._connected
        assert endpoint != ""
        self._connected = True

    def connect(self, endpoint):
        """Mock the socket connect method.
        """
        assert not self._connected
        assert endpoint != ""
        self._connected = True

    def close(self, linger):
        """Mock the socket close method.
        """
        # pylint: disable=unused-argument

        assert self._connected
        self._connected = False


class MockZmqContext(mock.MagicMock):
    """Mock a zmq context.
    """

    def __init__(self, **kwargs):
        super(MockZmqContext, self).__init__(**kwargs)
        self._destroyed = False
        self.IPV6 = None  # pylint: disable=invalid-name
        self.RCVTIMEO = None  # pylint: disable=invalid-name

    def socket(self, sock_type):
        """Mock a zmq socket call.
        """
        # pylint: disable=unused-argument

        assert not self._destroyed
#        assert self.IPV6 == 1
#        assert self.RCVTIMEO is not None
#        assert sock_type == zmq.REQ
        return MockZmqSocket()

    def destroy(self, linger=None):
        """Mock the context destroy method.
        """
        # pylint: disable=unused-argument

        assert not self._destroyed
        self._destroyed = True


class MockZmqPoller(mock.MagicMock):
    """Mock the zmq poller.
    """

    def __init__(self, **kwargs):
        super(MockZmqPoller, self).__init__(**kwargs)
        self.registered_sockets = []

        self.poll = mock.MagicMock()

    def register(self, socket, event):
        """Mock the poller register method.
        """
        assert isinstance(socket, zmq.sugar.socket.Socket)
        assert event in [zmq.POLLIN, zmq.POLLOUT, zmq.POLLERR]
        self.registered_sockets.append([socket, event])


class MockZmqPollerAllFake(mock.MagicMock):
    """Mock the zmq poller. All methods come from mock.
    """

    def __init__(self, **kwargs):
        super(MockZmqPollerAllFake, self).__init__(**kwargs)
        self.poll = mock.MagicMock()
        self.register = mock.MagicMock()


class MockZmqAuthenticator(mock.MagicMock):
    """Mock the zmq authenticator.
    """

    def __init__(self, **kwargs):
        super(MockZmqAuthenticator, self).__init__(**kwargs)

        self.start = mock.MagicMock()
        self.allow = mock.MagicMock()


class TestBase(unittest.TestCase):
    """The Base class from which all data fetchers should inherit from.
    """

    def setUp(self):
        global LOGLEVEL

        self.log_queue = False
        self.listener = None
        self.log = None
        self.context = None

        main_pid = os.getpid()
        self.con_ip = m_socket.getfqdn()
        self.ext_ip = m_socket.gethostbyaddr(self.con_ip)[2][0]
        ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")

        ports = {
            "com": 50000,
            "request": 50001,
            "request_fw": 50002,
            "router": 50004,
            "eventdet_port": 50003,
            "control_pub": 50005,
            "control_sub": 50006,
            "cleaner": 50051,
            "cleaner_trigger": 50052,
            "confirmation": 50053,
        }

        self.ipc_addresses = utils.set_ipc_addresses(ipc_dir=ipc_dir,
                                                     main_pid=main_pid)

        confirm_ips = [self.ext_ip, self.con_ip]

        endpoints = utils.set_endpoints(ext_ip=self.ext_ip,
                                        con_ip=self.con_ip,
                                        ports=ports,
                                        confirm_ips=confirm_ips,
                                        ipc_addresses=self.ipc_addresses)

        self.config = {
            "ports": ports,
            "ipc_dir": ipc_dir,
            "main_pid": main_pid,
            "endpoints": endpoints,
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

#        self.log = utils.get_logger("test_datafetcher", self.log_queue)
        self.log = MockLogging()

    def set_up_recv_socket(self, port):
        """Create pull socket and connect to port.

        Args:
            port: Port to connect to.
        """

        endpoint = "tcp://{}:{}".format(self.ext_ip, port)

        return self.start_socket(
            name="receiving_socket",
            sock_type=zmq.PULL,
            sock_con="bind",
            endpoint=endpoint
        )

    def start_socket(self, name, sock_type, sock_con, endpoint):
        """Wrapper of utils.start_socket
        """

        return utils.start_socket(
            name=name,
            sock_type=sock_type,
            sock_con=sock_con,
            endpoint=endpoint,
            context=self.context,
            log=self.log
        )

    def stop_socket(self, name, socket=None):
        """Wrapper for utils.stop_socket.
        """
        # use the class attribute
        if socket is None:
            socket = getattr(self, name)
            use_class_attribute = True
        else:
            use_class_attribute = False

        return_socket = utils.stop_socket(name=name,
                                          socket=socket,
                                          log=self.log)

        # class attributes are set directly
        if use_class_attribute:
            setattr(self, name, return_socket)
        else:
            return return_socket

    def tearDown(self):
        for _, endpoint in vars(self.ipc_addresses).iteritems():
            try:
                os.remove(endpoint)
                self.log.debug("Removed ipc socket: {}".format(endpoint))
            except OSError:
                pass
#                selfi.log.debug("Could not remove ipc socket: {}"
#                               .format(endpoint))
            except Exception:
                self.log.warning("Could not remove ipc socket: {}"
                                 .format(endpoint), exc_info=True)

        if self.listener is not None:
            self.log_queue.put_nowait(None)
            self.listener.stop()
            self.listener = None
