#!/usr/bin/env python

# Copyright (C) 201-2020  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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
A Checker to see if the receiver is still alive and answering.
"""

import argparse
from distutils.version import LooseVersion
import sys
import zmq

import hidra.utils as utils


class AliveTest(object):
    """ Connects to the receiver and checks that it is alive """

    def __init__(self, socket_id, log=utils.LoggingFunction()):
        self.context = None
        self.socket = None
        self.socket_id = socket_id
        self.log = log

        self.create_sockets()

    def create_sockets(self):
        """ Set up zmq sockets """
        endpoint = "tcp://{}".format(self.socket_id)

        try:
            self.context = zmq.Context()
            self.log.debug("Registering ZMQ context")

            self.socket = self.context.socket(zmq.PUSH)

            self.socket.connect(endpoint)
            self.log.info("Start socket (connect): '%s'", endpoint)
        except Exception:
            self.log.error("Failed to start socket (connect):'%s'", endpoint,
                           exc_info=True)

    def run(self, enable_logging=False):
        """ Run the testing """
        try:
            if enable_logging:
                self.log.debug("ZMQ version used: %s", zmq.__version__)

            return self._send_test(enable_logging=enable_logging)

        except Exception:
            self.log.error("Failed to send test message to host %s",
                           self.socket_id, exc_info=True)
            return False

    def _send_test(self, enable_logging=False):
        """ Sending the test signal and react the outcome """

        signal = [b"ALIVE_TEST"]

        # --------------------------------------------------------------------
        # old zmq version
        # --------------------------------------------------------------------
        # With older ZMQ versions the tracker results in an ZMQError in
        # the DataDispatchers when an event is processed
        # (ZMQError: Address already in use)
        if LooseVersion(zmq.__version__) <= LooseVersion("14.5.0"):

            self.socket.send_multipart(signal)
            if enable_logging:
                self.log.info("Sending test message to host %s...success",
                              self.socket_id)
            return True

        # --------------------------------------------------------------------
        # newer zmq version
        # --------------------------------------------------------------------
        # due to optimizations done in newer zmq version track needs
        # additional parameter to work properly again. See pyzmq issue #1364
        if LooseVersion(zmq.__version__) > LooseVersion("17.0.0"):
            self.socket.copy_threshold = 0

        tracker = self.socket.send_multipart(signal, copy=False, track=True)
        if not tracker.done:
            tracker.wait(2)
        if not tracker.done:
            self.log.error("Failed to send test message to host %s",
                           self.socket_id, exc_info=True)
            return False
        elif enable_logging:
            self.log.info("Sending test message to host %s...success",
                          self.socket_id)

        return True

    def stop(self):
        """ Stop and clean up """

        if self.socket:
            self.log.debug("Stopping socket")
            self.socket.close(0)
            self.socket = None

        if self.context:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()

    def __del__(self):
        self.stop()


def main():
    """ Parsing arguments and checking the receiver """

    parser = argparse.ArgumentParser()

    parser.add_argument("--host",
                        type=str,
                        help="IP/DNS-name the HiDRA receiver is bound to",
                        default="asap3-p00")
    parser.add_argument("--port",
                        type=int,
                        help="Port the HiDRA receiver is bound to",
                        default=50100)
    parser.add_argument("-v", "--verbose",
                        help="Logs details about the test",
                        action="store_true")

    args = parser.parse_args()

    socket_id = "{}:{}".format(args.host, args.port)

    if args.verbose:
        test = AliveTest(socket_id, utils.LoggingFunction())
    else:
        test = AliveTest(socket_id, utils.LoggingFunction(None))

    if test.run(args.verbose):
        print("Test successful")
        sys.exit(0)
    else:
        print("Test failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
