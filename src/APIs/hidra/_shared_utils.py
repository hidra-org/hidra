
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import traceback


class Base(object):
    def __init__(self):
        self.log = None
        self.context = None

    def _start_socket(self,
                      name,
                      sock_type,
                      sock_con,
                      endpoint,
                      is_ipv6=False,
                      zap_domain=None,
                      message=None):
        """Wrapper of start_socket.
        """
        return start_socket(
            name=name,
            sock_type=sock_type,
            sock_con=sock_con,
            endpoint=endpoint,
            context=self.context,
            log=self.log,
            is_ipv6=is_ipv6,
            zap_domain=zap_domain,
            message=message
        )

    def _stop_socket(self, name, socket=None):
        """Closes a zmq socket.

        Args:
            name: The name of the socket (used in log messages).
            socket: The ZMQ socket to be closed.
        """

        # use the class attribute
        if socket is None:
            socket = getattr(self, name)
            use_class_attribute = True
        else:
            use_class_attribute = False

        # close socket
        socket = stop_socket(name=name, socket=socket, log=self.log)

        # class attributes are set directly
        if use_class_attribute:
            setattr(self, name, socket)
        else:
            return socket


# ------------------------------ #
#         ZMQ functions          #
# ------------------------------ #

MAPPING_ZMQ_CONSTANTS_TO_STR = [
    "PAIR",  # zmq.PAIR = 0
    "PUB",  # zmq.PUB = 1
    "SUB",  # zmq.SUB = 2
    "REQ",  # zmq.REQ = 3
    "REP",  # zmq.REP = 4
    "DEALER/XREQ",  # zmq.DEALER/zmq.XREQ = 5
    "ROUTER/XREP",  # zmq.ROUTER/zmq.XREP = 6
    "PULL",  # zmq.PULL = 7
    "PUSH",  # zmq.PUSH = 8
    "XPUB",  # zmq.XPUB = 9
    "XSUB",  # zmq.XSUB = 10
]


def start_socket(name,
                 sock_type,
                 sock_con,
                 endpoint,
                 context,
                 log,
                 is_ipv6=False,
                 zap_domain=None,
                 message=None):
    """Creates a zmq socket.

    Args:
        name: The name of the socket (used in log messages).
        sock_type: ZMQ socket type (e.g. zmq.PULL).
        sock_con: ZMQ binding type (connect or bind).
        endpoint: ZMQ endpoint to connect to.
        context: ZMQ context to create the socket on.
        log: Logger used for log messages.
        is_ipv6: Enable IPv6 on socket.
        zap_domain: The RFC 27 authentication domain used for ZMQ
                    communication.
        message (optional): wording to be used in the message
                            (default: Start).
    """

    if message is None:
        message = "Start"

    sock_type_as_str = MAPPING_ZMQ_CONSTANTS_TO_STR[sock_type]

    try:
        # create socket
        socket = context.socket(sock_type)

        # register the authentication domain
        if zap_domain:
            socket.zap_domain = zap_domain

        # enable IPv6 on socket
        if is_ipv6:
            socket.ipv6 = True
            log.debug("Enabling IPv6 socket for {}".format(name))

        # connect/bind the socket
        if sock_con == "connect":
            socket.connect(endpoint)
        elif sock_con == "bind":
            socket.bind(endpoint)

        log.info("{} {} ({}, {}): '{}'".format(message,
                                               name,
                                               sock_con,
                                               sock_type_as_str,
                                               endpoint))
    except:
        log.error("Failed to {} {} ({}, {}): '{}'".format(name,
                                                          message.lower(),
                                                          sock_con,
                                                          sock_type_as_str,
                                                          endpoint),
                  exc_info=True)
        raise

    return socket


def stop_socket(name, socket, log):
    """Closes a zmq socket.

    Args:
        name: The name of the socket (used in log messages).
        socket: The ZMQ socket to be closed.
        log: Logger used for log messages.
    """

    # close socket
    if socket is not None:
        log.info("Closing {}".format(name))
        socket.close(linger=0)
        socket = None

    return socket


# ------------------------------ #
#            Logging             #
# ------------------------------ #

class LoggingFunction:

    def __init__(self, level="debug"):
        if level == "debug":
            # using output
            self.debug = self.out
            self.info = self.out
            self.warning = self.out
            self.error = self.out
            self.critical = self.out
        elif level == "info":
            # using no output
            self.debug = self.no_out
            # using output
            self.info = self.out
            self.warning = self.out
            self.error = self.out
            self.critical = self.out
        elif level == "warning":
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            # using output
            self.warning = self.out
            self.error = self.out
            self.critical = self.out
        elif level == "error":
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            self.warning = self.no_out
            # using output
            self.error = self.out
            self.critical = self.out
        elif level == "critical":
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            self.warning = self.no_out
            self.error = self.no_out
            # using output
            self.critical = self.out
        elif level is None:
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            self.warning = self.no_out
            self.error = self.no_out
            self.critical = self.no_out

    def out(self, x, exc_info=None):
        if exc_info:
            print(x, traceback.format_exc())
        else:
            print(x)

    def no_out(self, x, exc_info=None):
        pass


class LoggingFunctionOld:
    def out(self, x, exc_info=None):
        if exc_info:
            print(x, traceback.format_exc())
        else:
            print(x)

    def no_out(self, x, exc_info=None):
        pass

    def __init__(self, level="debug"):
        if level == "debug":
            # using output
            self.debug = lambda x, exc_info=None: self.out(x, exc_info)
            self.info = lambda x, exc_info=None: self.out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.out(x, exc_info)
            self.error = lambda x, exc_info=None: self.out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level == "info":
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            # using output
            self.info = lambda x, exc_info=None: self.out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.out(x, exc_info)
            self.error = lambda x, exc_info=None: self.out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level == "warning":
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.info = lambda x, exc_info=None: self.no_out(x, exc_info)
            # using output
            self.warning = lambda x, exc_info=None: self.out(x, exc_info)
            self.error = lambda x, exc_info=None: self.out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level == "error":
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.info = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.no_out(x, exc_info)
            # using output
            self.error = lambda x, exc_info=None: self.out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level == "critical":
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.info = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.error = lambda x, exc_info=None: self.no_out(x, exc_info)
            # using output
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level is None:
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.info = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.error = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.no_out(x, exc_info)
