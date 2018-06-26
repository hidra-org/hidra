from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import zmq
from collections import namedtuple

from eventdetectorbase import EventDetectorBase
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


IpcEndpoints = namedtuple("ipc_endpoints", ["eventdet"])
TcpEndpoints = namedtuple("tcp_endpoints", ["eventdet_bind", "eventdet_con"])
Addresses = namedtuple("addresses", ["eventdet_bind", "eventdet_con"])


def get_tcp_endpoints(config):
    """Build the endpoints used for TCP communcation.

    The endpoints are only set if called on Windows. For Linux they are set
    to None.

    Args:
        config (dict): A dictionary containing the IPs to bind and to connect
                       to as well as the ports. Usually con_ip is teh DNS name.
    Returns:
        A TcpEndpoints object.
    """

    if utils.is_windows():
        ext_ip = config["ext_ip"]
        con_ip = config["con_ip"]

        port = config["event_det_port"]
        eventdet_bind = "{}:{}".format(ext_ip, port)
        eventdet_con = "{}:{}".format(con_ip, port)

        endpoints = TcpEndpoints(
            eventdet_bind=eventdet_bind,
            eventdet_con=eventdet_con
        )
    else:
        endpoints = None

    return endpoints


def get_ipc_endpoints(config):
    """Build the endpoints used for IPC.

    The endpoints are only set if called on Linux. On windows they are set
    to None.

    Args:
        config (dict): A dictionary conaining the ipc base directory and the
                       main PID.
    Returns:
        An IpcEndpoints object.
    """
    if utils.is_windows():
        endpoints = None
    else:
        ipc_ip = "{}/{}".format(config["ipc_dir"],
                                config["main_pid"])

        eventdet = "{}_{}".format(ipc_ip, "eventDet")

        endpoints = IpcEndpoints(eventdet=eventdet)

    return endpoints


def get_addrs(ipc_endpoints, tcp_endpoints):
    """Configures the ZMQ address depending on the protocol.

    Args:
        ipc_endpoints: The endpoints used for the interprocess communication
                       (ipc) protocol.
        tcp_endpoints: The endpoints used for communication over TCP.
    Returns:
        An Addresses object containing the bind and connection addresses.
    """

    if ipc_endpoints is not None:
        eventdet_bind = "ipc://{}".format(ipc_endpoints.eventdet)
        eventdet_con = eventdet_bind

    elif tcp_endpoints is not None:
        eventdet_bind = "tcp://{}".format(tcp_endpoints.eventdet_bind)
        eventdet_con = "tcp://{}".format(tcp_endpoints.eventdet_con)
    else:
        msg = "Neither ipc not tcp endpoints are defined"
        raise Exception(msg)

    return Addresses(
        eventdet_bind=eventdet_bind,
        eventdet_con=eventdet_con
    )


# generalization not used because removing the ipc_endpoints later is more
# difficult
# Endpoints = namedtuple("endpoints", ["eventdet_bind", "eventdet_con"])
# def get_endpoints(config):
#    if not utils.is_windows():
#        eventdet_bind = "{}:{}".format(config["ext_ip"],
#                                       config["event_det_port"]),
#        eventdet_con = "{}:{}".formau(config["con_ip"],
#                                      config["event_det_port"]),
#    else:
#        ipc_ip = "{}/{}".format(config["ipc_dir"],
#                                config["main_pid"])
#
#        eventdet_bind = "{}_{}".format(ipc_ip, "eventDet"),
#        eventdet_con = eventdet_bind
#
#    endpoints = Endpoints(
#        eventdet_bind=eventdet_bind,
#        eventdet_con=eventdet_con
#    )
#
#    return endpoints
#
#
# def get_addrs(config, endpoints):
#
#    if utils.is_windows():
#        protocol = "tcp"
#    else:
#        protocol = "ipc"
#
#    eventdet_bind = "{}://{}".format(protocol, endpoints.eventdet_bind)
#    eventdet_con = "{}://{}".format(protocol, endpoints.eventdet_con)
#
#    addrs = Addresses(
#        eventdet_bind=eventdet_bind,
#        eventdet_con=eventdet_con
#    )
#
#    return addrs


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config,
                                   log_queue,
                                   "zmq_events")

        self.config = config
        self.log_queue = log_queue

        self.ext_context = None
        self.context = None
        self.event_socket = None

        self.ipc_endpoints = None
        self.tcp_endpoints = None
        self.addrs = None

        self.set_required_params()

        self.check_config()
        self.setup()

    def set_required_params(self):
        """
        Defines the parameters to be in configuration to run this datafetcher.
        Depending if on Linux or Windows other parameters are required.
        """

        self.required_params = ["context", "number_of_streams"]
        if utils.is_windows():
            self.required_params += ["ext_ip", "event_det_port"]
        else:
            self.required_params += ["ipc_dir", "main_pid"]

    def setup(self):
        """
        Sets ZMQ endpoints and addresses and creates the ZMQ socket.
        """

        self.ipc_endpoints = get_ipc_endpoints(config=self.config)
        self.tcp_endpoints = get_tcp_endpoints(config=self.config)
        self.addrs = get_addrs(ipc_endpoints=self.ipc_endpoints,
                               tcp_endpoints=self.tcp_endpoints)

        # remember if the context was created outside this class or not
        if self.config["context"]:
            self.context = self.config["context"]
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

        # Create zmq socket to get events
        try:
            self.event_socket = self.context.socket(zmq.PULL)
            self.event_socket.bind(self.addrs.eventdet_bind)
            self.log.info("Start event_socket (bind): '{}'"
                          .format(self.addrs.eventdet_bind))
        except:
            self.log.error("Failed to start event_socket (bind): '{}'"
                           .format(self.addrs.eventdet_bind), exc_info=True)
            raise

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """

        event_message = self.event_socket.recv_multipart()

        if event_message[0] == b"CLOSE_FILE":
            event_message_list = [
                event_message for i in range(self.config["number_of_streams"])
            ]
        else:
            event_message_list = [json.loads(event_message[0].decode("utf-8"))]

        self.log.debug("event_message: {}".format(event_message_list))

        return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """
        # close ZMQ
        if self.event_socket is not None:
            self.event_socket.close(0)
            self.event_socket = None

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.ext_context and self.context is not None:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)
