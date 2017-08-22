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
        self.debug = lambda x, exc_info=None: self.out(x, exc_info)
        self.info = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning = lambda x, exc_info=None: self.out(x, exc_info)
        self.error = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


class NoLoggingFunction:
    def out(self, x, exc_info=None):
        pass

    def __init__(self):
        self.debug = lambda x, exc_info=None: self.out(x, exc_info)
        self.info = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning = lambda x, exc_info=None: self.out(x, exc_info)
        self.error = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


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
            con_str = "tcp://{0}".format(self.socket_id)

            self.socket.connect(con_str)
            self.log.info("Start socket (connect): '{0}'".format(con_str))
        except:
            self.log.error("Failed to start socket (connect):'{0}'"
                           .format(con_str), exc_info=True)

    def run(self, enable_logging=False):
        try:
            if enable_logging:
                self.log.debug("ZMQ version used: {0}".format(zmq.__version__))

            # With older ZMQ versions the tracker results in an ZMQError in
            # the DataDispatchers when an event is processed
            # (ZMQError: Address already in use)
            if zmq.__version__ <= "14.5.0":

                self.socket.send_multipart([b"ALIVE_TEST"])
                if enable_logging:
                    self.log.info("Sending test message to host {0}...success"
                                  .format(self.socket_id))

            else:
                tracker = self.socket.send_multipart([b"ALIVE_TEST"],
                                                     copy=False,
                                                     track=True)
                if not tracker.done:
                    tracker.wait(2)
                if not tracker.done:
                    self.log.error("Failed to send test message to host {0}"
                                   .format(self.socket_id), exc_info=True)
                    return False
                elif enable_logging:
                    self.log.info("Sending test message to host {0}...success"
                                  .format(self.socket_id))
            return True
        except:
            self.log.error("Failed to send test message to host {0}"
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

    def __exit__(self):
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

    args = parser.parse_args()

    socket_id = "{0}:{1}".format(args.host, args.port)

    #test = AliveTest(socket_id)
    test = AliveTest(socket_id, NoLoggingFunction())

    if test.run():
        print("Test successfull")
        sys.exit(0)
    else:
        print("Test failed")
        sys.exit(1)

