import utils


# ------------------------------ #
#  Base class for ZMQ handling   #
# ------------------------------ #

class Base(object):
    def __init__(self):
        self.log = None
        self.context = None

    def start_socket(self, name, sock_type, sock_con, endpoint, message=None):
        """Wrapper of start_socket
        """

        return utils.start_socket(
            name=name,
            sock_type=sock_type,
            sock_con=sock_con,
            endpoint=endpoint,
            context=self.context,
            log=self.log,
            message=message,
        )

    def stop_socket(self, name, socket=None):
        """Wrapper for stop_socket.
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
