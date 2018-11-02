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

"""
API to communicate with a hidra control server or with a hidra receiver.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
import logging
import os
import socket
import sys
import zmq

# from ._version import __version__
from ._constants import connection_list
from ._shared_utils import (
    CommunicationFailed,
    LoggingFunction,
    Base,
    execute_ldapsearch
)


def check_netgroup(hostname, beamline, ldapuri, netgroup_template, log=None):
    """Check if a host is in a netgroup belonging to a certain beamline.

    Args:
        hostname: The host to check.
        beamline: The beamline to which the host should belong to.
        ldapuri: Ldap node and port needed to check whitelist.
        netgroup_template: A template of the netgroup.
        log (optional): if the result should be logged.
                        None: no logging
                        False: use print
                        anything else: use the standard logging module
    """

    if log is None:
        log = LoggingFunction(None)
    elif log:
        pass
    else:
        log = LoggingFunction("debug")

    netgroup_name = netgroup_template.format(bl=beamline)
    netgroup = execute_ldapsearch(log, netgroup_name, ldapuri)

    # convert host to fully qualified DNS name
    hostname = socket.getfqdn(hostname)

    if hostname not in netgroup:
        log.error("Host {} is not contained in netgroup of "
                  "beamline {}".format(hostname, beamline))
        sys.exit(1)


class Control(Base):
    """Communicate with a hidra control server.
    """

    def __init__(self,
                 beamline,
                 detector,
                 ldapuri,
                 netgroup_template,
                 use_log=False):
        """
        Enables controlling like starting/stopping or setting of parameters of
        a HiDRA instance. Such an instance is started for a specific beamline
        together with a specific detector.

        Args:
            beamline: The beamline for which the HiDRA instance should be
                      controlled.
            detector: The detector for which the HiDRA instance should be
                      controlled.
            ldapuri: The LDAP uri (<host>:<port>) to connect to check for
                     authentication.
            netgroup_template: The template to be used for netgroup
                               checking of the beamline (e.g. a3{bl}-hosts).
            use_log (optional): Specified the logging type.
        """

        super(Control, self).__init__()

        self.beamline = beamline
        self.detector = detector
        self.ldapuri = ldapuri
        self.netgroup_template = netgroup_template
        self.use_log = use_log

        self.log = None
        self.current_pid = None
        self.host = None
        self.context = None
        self.socket = None

        self._setup()

    def _setup(self):

        # pylint: disable=redefined-variable-type

        # print messages of certain level to screen
        if self.use_log in ["debug", "info", "warning", "error", "critical"]:
            self.log = LoggingFunction(self.use_log)

        # use logging
        elif self.use_log:
            self.log = logging.getLogger("Control")

        # use no logging at all
        elif self.use_log is None:
            self.log = LoggingFunction(None)

        # print everything to screen
        else:
            self.log = LoggingFunction("debug")

        self.current_pid = os.getpid()
        self.host = socket.getfqdn()

        # check host running control script
        check_netgroup(self.host,
                       self.beamline,
                       self.ldapuri,
                       self.netgroup_template,
                       self.log)
        # check detector
        check_netgroup(self.detector,
                       self.beamline,
                       self.ldapuri,
                       self.netgroup_template,
                       self.log)

        try:
            endpoint = "tcp://{}:{}".format(
                connection_list[self.beamline]["host"],
                connection_list[self.beamline]["port"]
            )
            self.log.info("Starting connection to {}".format(endpoint))
        except KeyError:
            self.log.error("Beamline {} not supported".format(self.beamline))
            sys.exit(1)

        # Create ZeroMQ context
        self.log.info("Registering ZMQ context")
        self.context = zmq.Context()

        # socket to get requests
        self.socket = self._start_socket(
            name="socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint=endpoint
        )

        self._check_responding()

    def _check_responding(self):
        """ Check if the control server is responding.
        """

        test_signal = b"IS_ALIVE"
        tracker = self.socket.send_multipart([test_signal],
                                             zmq.NOBLOCK,
                                             copy=False,
                                             track=True)

        # test if someone picks up the test message in the next 2 sec
        if not tracker.done:
            try:
                tracker.wait(1)
            except zmq.error.NotDone:
                pass

        # no one picked up the test message
        if not tracker.done:
            self.log.error("HiDRA control server is not answering.")
            self.stop(unregister=False)
            sys.exit(1)

        responce = self.socket.recv()
        if responce == b"OK":
            self.log.info("HiDRA control server up and answering.")
        else:
            self.log.error("HiDRA control server is in failed state.")
            self.log.debug("responce was: {}".format(responce))
            self.stop(unregister=False)
            sys.exit(1)

    def get(self, attribute, timeout=None):
        """Get the value of an attribute.

        Args:
            attribute: The attribute of which the value should be requested.
            timeout (optional): How long to wait for an answer.

        Return:
            Value of the attribute.
        """

        # pylint: disable=unused-argument

        msg = [b"get", self.host, self.detector, attribute]

        self.socket.send_multipart(msg)
        self.log.debug("sent: {}".format(msg))

        # TODO implement timeout
        reply = self.socket.recv()
        self.log.debug("recv: {}".format(reply))

        return json.loads(reply)

    def set(self, attribute, *value):
        """Set the value of an attribute.

        Args:
            attribute: The attribute to set the value of.
            value: The value the attribute should be set to.

        Return:
            Received "DONE" if setting was successful and "ERROR" if not.
        """

        # flatten list if entry was a list (result: list of lists)
        if isinstance(value[0], list):
            value = [item for sublist in value for item in sublist]
        else:
            value = value[0]

        if attribute == "det_ip":
            check_netgroup(value,
                           self.beamline,
                           self.ldapuri,
                           self.netgroup_template,
                           self.log)

        msg = [b"set", self.host, self.detector, attribute,
               json.dumps(value)]

        self.socket.send_multipart(msg)
        self.log.debug("sent: {}".format(msg))

        reply = self.socket.recv()
        self.log.debug("recv: {}".format(reply))

        return reply

    def do(self, command, timeout=None):  # pylint: disable=invalid-name
        """Request the server to execute a command.

        Args:
            command: Command to execute (e.g. start, stop, status,...).
            timeout (optional): How long to wait for an answer.

        Return:
            Received "DONE" if execution was successful and "ERROR" if not.
            Some command can have additional return values:
            - start: "ALREADY_RUNNING"
            - stop: "ARLEADY_STOPPED"
            - status: "RUNNING", "NOT RUNNING"
        """

        # pylint: disable=unused-argument

        msg = [b"do", self.host, self.detector, command]

        self.socket.send_multipart(msg)
        self.log.debug("sent: {}".format(msg))

        # TODO implement timeout
        reply = self.socket.recv()
        self.log.debug("recv: {}".format(reply))

        return reply

    def stop(self, unregister=True):
        """Unregisters from server and cleans up sockets.

        Args:
            unregister (optional): If this client should be unregistered
                                   from the server.
        """

        if self.socket is not None:
            if unregister:
                self.log.info("Sending close signal")
                msg = [b"bye", self.host, self.detector]

                self.socket.send_multipart(msg)
                self.log.debug("sent: {}".format(msg))

                reply = self.socket.recv()
                self.log.debug("recv: {} ".format(reply))

            try:
                self._stop_socket(name="socket")
            except Exception:
                self.log.error("closing sockets...failed.", exc_info=True)

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


class ReceiverControl(Base):
    """Communicate with a hidra receiver.
    """

    def __init__(self, host, port=None):
        """
        Args:
            host: Host the receiver is running on.
            port (optional): Port the receiver is listening to.
        """

        super(ReceiverControl, self).__init__()

        if port is None:
            port = 50050

        self.log = None
        self.context = None
        self.status_socket = None
        self.poller = None

        self._setup(host, port)

    def _setup(self, host, port):

        # use no logging but print
        self.log = LoggingFunction(None)

        self.context = zmq.Context()

        self.status_socket = self._start_socket(
            name="status_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint="tcp://{}:{}".format(host, port)
        )

        self.timeout = 2000
        self.poller = zmq.Poller()
        self.poller.register(self.status_socket, zmq.POLLIN)

    def _get_response(self):
        try:
            socks = dict(self.poller.poll(self.timeout))
        except:
            self.log.error("Could not poll for new message")
            raise

        # if there was a response
        if (self.status_socket in socks
                and socks[self.status_socket] == zmq.POLLIN):

            response = self.status_socket.recv_multipart()
            self.log.debug("Response: {}".format(response))

            return response
        else:
            raise CommunicationFailed("No response received in time.")

    def get_status(self):
        """Get the status of the receiver.

        Return:
            The status of the receiver as string.
            "OK": Everything is ok.
            "ERROR": The receiver is in error state.
        """

        self.status_socket.send_multipart([b"STATUS_CHECK"])
        self.log.debug("Reset request sent")

        response = self._get_response()

        return response

    def reset_status(self):
        """Reset the status flag of the receiver.

        After an error occurred during data receiving the receiver flags it.
        Reset is only done by external trigger.
        """

        self.status_socket.send_multipart([b"RESET_STATUS"])
        self.log.debug("Reset request sent")

        self._get_response()

    def stop(self):
        """Clean up zmq part.
        """

        if self.context is not None:
            self.context.destroy(0)

        self._stop_socket(name="status_socket")

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()
