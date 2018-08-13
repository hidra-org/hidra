#!/usr/bin/env python

import argparse
#import nagiosplugin
import sys
import traceback
import zmq

class LoggingFunction:
    def out(self, x, exc_info=None):
        if exc_info:
            print(x, traceback.format_exc())
        else:
            print(x)

    def __init__(self):
        self.debug = self.out
        self.info = self.out
        self.warning = self.out
        self.error = self.out
        self.critical = self.out


class NoLoggingFunction:
    def out(self, x, exc_info=None):
        pass

    def __init__(self):
        self.debug = self.out
        self.info = self.out
        self.warning = self.out
        self.error = self.out
        self.critical = self.out


class AliveTest():
    def __init__(self, socket_id, log=LoggingFunction()):
        self.context = None
        self.socket = None
        self.socket_id = socket_id
        self.log = log

        self.create_sockets()

    def create_sockets(self):
        try:
            self.context = zmq.Context()
            self.log.debug("Registering ZMQ context")

            self.socket = self.context.socket(zmq.PUSH)
            endpoint = "tcp://{}".format(self.socket_id)

            self.socket.connect(endpoint)
            self.log.info("Start socket (connect): '{}'".format(endpoint))
        except:
            self.log.error("Failed to start socket (connect):'{}'"
                           .format(enpoint), exc_info=True)

    def run(self, enable_logging=False):
        try:
            if enable_logging:
                self.log.debug("ZMQ version used: {}".format(zmq.__version__))

            # With older ZMQ versions the tracker results in an ZMQError in
            # the DataDispatchers when an event is processed
            # (ZMQError: Address already in use)
            if zmq.__version__ <= "14.5.0":

                self.socket.send_multipart([b"ALIVE_TEST"])
                if enable_logging:
                    self.log.info("Sending test message to host {}...success"
                                  .format(self.socket_id))

            else:
                tracker = self.socket.send_multipart([b"ALIVE_TEST"],
                                                     copy=False,
                                                     track=True)
                if not tracker.done:
                    tracker.wait(2)
                if not tracker.done:
                    self.log.error("Failed to send test message to host {}"
                                   .format(self.socket_id), exc_info=True)
                    return False
                elif enable_logging:
                    self.log.info("Sending test message to host {}...success"
                                  .format(self.socket_id))
            return True
        except:
            self.log.error("Failed to send test message to host {}"
                           .format(self.socket_id), exc_info=True)
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

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()

if __name__ == '__main__':

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

    test = None
    if args.verbose:
        test = AliveTest(socket_id, LoggingFunction())
    else:
        test = AliveTest(socket_id, NoLoggingFunction())

    if test.run(args.verbose):
        print("Test successfull")
        sys.exit(0)
    else:
        print("Test failed")
        sys.exit(1)
