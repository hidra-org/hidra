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
        """Creates a zmq socket.

        Args:
            name: The name of the socket (used in log messages).
            sock_type: ZMQ socket type (e.g. zmq.PULL).
            sock_con: ZMQ binding type (connect or bind).
            endpoint: ZMQ endpoint to connect to.
            is_ipv6: Enable IPv6 on socket.
            zap_domain: The RFC 27 authentication domain used for ZMQ
                        communication.
            message (optional): wording to be used in the message
                                (default: Start).
        """

        if message is None:
            message = "Start"

        try:
            # create socket
            socket = self.context.socket(sock_type)

            # register the authentication domain
            if zap_domain:
                socket.zap_domain = zap_domain

            # enable IPv6 on socket
            if is_ipv6:
                socket.ipv6 = True
                self.log.debug("Enabling IPv6 socket for {}".format(name))

            # connect/bind the socket
            if sock_con == "connect":
                socket.connect(endpoint)
            elif sock_con == "bind":
                socket.bind(endpoint)

            self.log.info("{} {} ({}): '{}'"
                          .format(message, name, sock_con, endpoint))
        except:
            self.log.error("Failed to {} {} ({}): '{}'"
                           .format(name, message.lower(), sock_con, endpoint),
                           exc_info=True)
            raise

        return socket

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
        if socket is not None:
            self.log.info("Closing {}".format(name))
            socket.close(linger=0)
            socket = None

        # class attributes are set directly
        if use_class_attribute:
            setattr(self, name, socket)
        else:
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
