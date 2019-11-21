#!/usr/bin/env python

import argparse
from distutils.version import LooseVersion
import sys
import zmq

import hidra.utils as utils


class AliveTest(object):
    def __init__(self, socket_id, log=utils.LoggingFunction()):
        self.context = None
        self.socket = None
        self.socket_id = socket_id
        self.log = log

        self.create_sockets()

    def create_sockets(self):
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
        try:
            if enable_logging:
                self.log.debug("ZMQ version used: %s", zmq.__version__)

            # With older ZMQ versions the tracker results in an ZMQError in
            # the DataDispatchers when an event is processed
            # (ZMQError: Address already in use)
            if LooseVersion(zmq.__version__) <= LooseVersion("14.5.0"):

                self.socket.send_multipart([b"ALIVE_TEST"])
                if enable_logging:
                    self.log.info("Sending test message to host %s...success",
                                  self.socket_id)

            else:
                tracker = self.socket.send_multipart([b"ALIVE_TEST"],
                                                     copy=False,
                                                     track=True)
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
        except Exception:
            self.log.error("Failed to send test message to host %s",
                           self.socket_id, exc_info=True)
            return False

    def stop(self):
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
        test = AliveTest(socket_id, utils.NoLoggingFunction())

    if test.run(args.verbose):
        print("Test successful")
        sys.exit(0)
    else:
        print("Test failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
