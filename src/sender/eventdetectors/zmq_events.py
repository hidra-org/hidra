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
This module implements an event detector to be used together with the hidra
ingest API.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple
import json
import zmq

from eventdetectorbase import EventDetectorBase
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


IpcAddresses = namedtuple("ipc_addresses", ["eventdet"])
TcpAddresses = namedtuple("tcp_addresses", ["eventdet_bind", "eventdet_con"])
Endpoints = namedtuple("endpoints", ["eventdet_bind", "eventdet_con"])


def get_tcp_addresses(config):
    """Build the addresses used for TCP communcation.

    The addresses are only set if called on Windows. For Linux they are set
    to None.

    Args:
        config (dict): A dictionary containing the IPs to bind and to connect
                       to as well as the ports. Usually con_ip is the DNS name.
    Returns:
        A TcpAddresses object.
    """

    if utils.is_windows():
        ext_ip = config["network"]["ext_ip"]
        con_ip = config["network"]["con_ip"]

        port = config["eventdetector"]["zmq_events"]["eventdetector_port"]
        eventdet_bind = "{}:{}".format(ext_ip, port)
        eventdet_con = "{}:{}".format(con_ip, port)

        addrs = TcpAddresses(
            eventdet_bind=eventdet_bind,
            eventdet_con=eventdet_con
        )
    else:
        addrs = None

    return addrs


def get_ipc_addresses(config):
    """Build the addresses used for IPC.

    The addresses are only set if called on Linux. On windows they are set
    to None.

    Args:
        config (dict): A dictionary conaining the ipc base directory and the
                       main PID.
    Returns:
        An IpcAddresses object.
    """
    if utils.is_windows():
        addrs = None
    else:
        ipc_ip = "{}/{}".format(config["network"]["ipc_dir"],
                                config["network"]["main_pid"])

        eventdet = "{}_{}".format(ipc_ip, "eventdet")

        addrs = IpcAddresses(eventdet=eventdet)

    return addrs


def get_endpoints(ipc_addresses, tcp_addresses):
    """Configures the ZMQ endpoints depending on the protocol.

    Args:
        ipc_addresses: The endpoints used for the interprocess communication
                       (ipc) protocol.
        tcp_addresses: The endpoints used for communication over TCP.
    Returns:
        An Endpoints object containing the bind and connection endpoints.
    """

    if ipc_addresses is not None:
        eventdet_bind = "ipc://{}".format(ipc_addresses.eventdet)
        eventdet_con = eventdet_bind

    elif tcp_addresses is not None:
        eventdet_bind = "tcp://{}".format(tcp_addresses.eventdet_bind)
        eventdet_con = "tcp://{}".format(tcp_addresses.eventdet_con)
    else:
        msg = "Neither ipc not tcp endpoints are defined"
        raise Exception(msg)

    return Endpoints(
        eventdet_bind=eventdet_bind,
        eventdet_con=eventdet_con
    )


# generalization not used because removing the ipc_addresses later is more
# difficult
# Addresses = namedtuple("addresses", ["eventdet_bind", "eventdet_con"])
# def get_addresses(config):
#    if not utils.is_windows():
#        eventdet_bind = "{}:{}".format(config["ext_ip"],
#                                       config["eventdetector_port"]),
#        eventdet_con = "{}:{}".formau(config["con_ip"],
#                                      config["eventdetector_port"]),
#    else:
#        ipc_ip = "{}/{}".format(config["ipc_dir"],
#                                config["main_pid"])
#
#        eventdet_bind = "{}_{}".format(ipc_ip, "eventdet"),
#        eventdet_con = eventdet_bind
#
#    addrs = Addresses(
#        eventdet_bind=eventdet_bind,
#        eventdet_con=eventdet_con
#    )
#
#    return addrs
#
#
# def get_endpoints(config, addrs):
#
#    if utils.is_windows():
#        protocol = "tcp"
#    else:
#        protocol = "ipc"
#
#    eventdet_bind = "{}://{}".format(protocol, addrs.eventdet_bind)
#    eventdet_con = "{}://{}".format(protocol, addrs.eventdet_con)
#
#    endpoints = Endpoints(
#        eventdet_bind=eventdet_bind,
#        eventdet_con=eventdet_con
#    )
#
#    return endpoints


class EventDetector(EventDetectorBase):
    """
    Implementation of the event detector to be used with the ingest API.
    """

    def __init__(self, eventdetector_base_config):

        # needs to be initialized before parent init
        # reason: if the latter fails stop would otherwise run into problems
        self.ext_context = None
        self.context = None
        self.event_socket = None

        EventDetectorBase.__init__(self, eventdetector_base_config,
                                   name=__name__)

        # base class sets
        #   self.config_all - all configurations
        #   self.config_ed - the config of the event detector
        #   self.config - the module specific config
        #   self.ed_type -  the name of the eventdetector module
        #   self.log_queue
        #   self.log

        self.ext_context = None
        self.context = None
        self.event_socket = None

        self.ipc_addresses = None
        self.tcp_addresses = None
        self.endpoints = None

        self.set_required_params()

        # check that the required_params are set inside of module specific
        # config
        self.check_config()
        self.setup()

    def set_required_params(self):
        """
        Defines the parameters to be in configuration to run this datafetcher.
        Depending if on Linux or Windows other parameters are required.
        """

        self.required_params = {
            "eventdetector": {self.ed_type: ["context", "number_of_streams"]},
            "network": ["context"]
        }

        if utils.is_windows():
            self.required_params["network"] += ["ext_ip"]

            ed_params = self.required_params["eventdetector"][self.ed_type]
            ed_params += ["eventdetector_port"]
        else:
            self.required_params["network"] += ["ipc_dir", "main_pid"]

    def setup(self):
        """
        Sets ZMQ endpoints and addresses and creates the ZMQ socket.
        """

        self.ipc_addresses = get_ipc_addresses(config=self.config_all)
        self.tcp_addresses = get_tcp_addresses(config=self.config_all)
        self.endpoints = get_endpoints(ipc_addresses=self.ipc_addresses,
                                       tcp_addresses=self.tcp_addresses)

        # remember if the context was created outside this class or not
        if self.config_all["network"]["context"]:
            self.context = self.config_all["network"]["context"]
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

        # Create zmq socket to get events
        self.event_socket = self.start_socket(
            name="event_socket",
            sock_type=zmq.PULL,
            sock_con="bind",
            endpoint=self.endpoints.eventdet_bind
        )

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """

        event_message = self.event_socket.recv_multipart()

        if event_message[0] == b"CLOSE_FILE":
            n_streams = self.config["number_of_streams"]
            event_message_list = [event_message for _ in range(n_streams)]
        else:
            event_message_list = [json.loads(event_message[0].decode("utf-8"))]

        self.log.debug("event_message: %s", event_message_list)

        return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """
        # close ZMQ
        self.stop_socket(name="event_socket")

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.ext_context and self.context is not None:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except Exception:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)
