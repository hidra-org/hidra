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
This module implements an event detector to connect multiple hidra instances
in series.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple
import copy
import json
import multiprocessing
import zmq
# from zmq.devices.monitoredqueuedevice import ThreadMonitoredQueue
from zmq.utils.strtypes import asbytes

from eventdetectorbase import EventDetectorBase
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


IpcAddresses = namedtuple("ipc_addresses", ["out", "mon"])
Endpoints = namedtuple("addresses", ["in_bind", "in_con",
                                     "out_bind", "out_con",
                                     "mon_bind", "mon_con"])


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

        out = "{}_{}".format(ipc_ip, "out")
        mon = "{}_{}".format(ipc_ip, "mon")

        addrs = IpcAddresses(out=out, mon=mon)

    return addrs


def get_endpoints(config, ipc_addresses):
    """Configures the ZMQ endpoints depending on the protocol.

    Args:
        config (dict): A dictionary containing the IPs to bind and to connect
                       to as well as the ports. Usually con_ip is teh DNS name.
        ipc_addresses: The addresses used for the interprocess communication
                       (ipc) protocol.
    Returns:
        An Endpoints object containing the bind and connection endpoints.
    """

    ext_ip = config["network"]["ext_ip"]
    con_ip = config["network"]["con_ip"]

    port = config["eventdetector"]["hidra_events"]["ext_data_port"]
    in_bind = "tcp://{}:{}".format(ext_ip, port)
    in_con = "tcp://{}:{}".format(con_ip, port)

    if utils.is_windows():
        port = config["datafetcher_port"]
        out_bind = "tcp://{}:{}".format(ext_ip, port)
        out_con = "tcp://{}:{}".format(con_ip, port)

        port = config["event_det_port"]
        mon_bind = "tcp://{}:{}".format(ext_ip, port)
        mon_con = "tcp://{}:{}".format(con_ip, port)
    else:
        out_bind = "ipc://{}".format(ipc_addresses.out)
        out_con = out_bind

        mon_bind = "ipc://{}".format(ipc_addresses.mon)
        mon_con = mon_bind

    return Endpoints(
        in_bind=in_bind, in_con=in_con,
        out_bind=out_bind, out_con=out_con,
        mon_bind=mon_bind, mon_con=mon_con
    )


class MonitorDevice(object):
    """
    A device to monitore a ZMQ queue for incoming data but excluding
    'ALIVE_TEST' messages.
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, in_endpoint, out_endpoint, mon_endpoint):

        self.in_prefix = asbytes('in')
        self.out_prefix = asbytes('out')

        self.context = zmq.Context()

        self.in_socket = self.context.socket(zmq.PULL)
        self.in_socket.bind(in_endpoint)

        self.out_socket = self.context.socket(zmq.PUSH)
        self.out_socket.bind(out_endpoint)

        self.mon_socket = self.context.socket(zmq.PUSH)
        self.mon_socket.bind(mon_endpoint)

        self.run()

    def run(self):
        """Forward messages received on the in_socket to the out_socket.

        In addition to forwarding the messages a notifycation is sent to the
        mon_socket. And 'ALIVE_TEST' messages are ignored in total.
        """

        while True:
            try:
                msg = self.in_socket.recv_multipart()
#                print ("[MonitoringDevice] In: Received message {}"
#                        .format(msg[0][:20]))
            except KeyboardInterrupt:
                break

            if msg != [b'ALIVE_TEST']:

                try:
                    mon_msg = [self.in_prefix] + msg
                    self.mon_socket.send_multipart(mon_msg)
#                    print ("[MonitoringDevice] Mon: Sent message")

                    self.out_socket.send_multipart(msg)
#                    print ("[MonitoringDevice] Out: Sent message {}"
#                            .format([msg[0], msg[1][:20]]))

                    mon_msg = [self.out_prefix] + msg
                    self.mon_socket.send_multipart(mon_msg)
#                    print ("[MonitoringDevice] Mon: Sent message")
                except KeyboardInterrupt:
                    break


class EventDetector(EventDetectorBase):
    """
    Implementation of the event detector reacting on data sent by another
    hidra instance.
    """

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config,
                                   log_queue,
                                   "hidra_events")
        # base class sets
        #   self.config_all - all configurations
        #   self.config_ed - the config of the event detector
        #   self.config - the module specific config
        #   self.ed_type -  the name of the eventdetector module
        #   self.log_queue
        #   self.log

        self.ipc_addresses = None
        self.endpoints = None

        self.context = None
        self.ext_context = None
        self.monitoringdevice = None
        self.mon_socket = None

        self.set_required_params()

        self.check_config()
        self.setup()

    def set_required_params(self):
        """
        Defines the parameters to be in configuration to run this datafetcher.
        Depending if on Linux or Windows other parameters are required.
        """

#        self.required_params = ["context", "ext_ip", "con_ip", "ext_data_port"]
#        if utils.is_windows():
#            self.required_params += ["event_det_port", "data_fetch_port"]
#        else:
#            self.required_params += ["ipc_dir", "main_pid"]


        self.required_params = {
            "eventdetector": {self.ed_type: ["ext_data_port"]},
            "network": ["ext_ip", "con_ip", "context"]
        }

        if utils.is_windows():
            ed_params = self.required_params["eventdetector"][self.ed_type]
            ed_params += ["event_det_port", "data_fetch_port"]
        else:
            self.required_params["network"] += ["ipc_dir", "main_pid"]

    def setup(self):
        """Configures ZMQ sockets and starts monitoring device.
        """

        self.ipc_addresses = get_ipc_addresses(config=self.config_all)
        self.endpoints = get_endpoints(config=self.config_all,
                                       ipc_addresses=self.ipc_addresses)

        # Set up monitored queue to get notification when new data is sent to
        # the zmq queue

        self.monitoringdevice = multiprocessing.Process(
            target=MonitorDevice,
            args=(self.endpoints.in_bind,
                  self.endpoints.out_bind,
                  self.endpoints.mon_bind)
        )

        # original monitored queue from pyzmq is not working
        # > in_prefix = asbytes('in')
        # > out_prefix = asbytes('out')
        #
        # > self.monitoringdevice = ThreadMonitoredQueue(
        # >   #   in       out      mon
        # >   zmq.PULL, zmq.PUSH, zmq.PUB, in_prefix, out_prefix)
        #
        # > self.monitoringdevice.bind_in(self.endpoints.in_bind)
        # > self.monitoringdevice.bind_out(self.endpoints.out_bind)
        # > self.monitoringdevice.bind_mon(self.endpoints.mon_bind)

        self.monitoringdevice.start()
        self.log.info("Monitoring device has started with (bind)\n"
                      "in: %s\nout: %s\nmon: %s", self.endpoints.in_bind,
                      self.endpoints.out_bind, self.endpoints.mon_bind)

        # set up monitoring socket where the events are sent to
        if self.config_all["network"]["context"] is not None:
            self.context = self.config_all["network"]["context"]
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

        # Create zmq socket to get events
        self.mon_socket = self.start_socket(
            name="mon_socket",
            sock_type=zmq.PULL,
            sock_con="connect",
            endpoint=self.endpoints.mon_con
        )
#        self.mon_socket.setsockopt_string(zmq.SUBSCRIBE, "")

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """

        self.log.debug("waiting for new event")
        message = self.mon_socket.recv_multipart()
        # the messages received are of the form
        # ['in', '<metadata dict>', <data>]
        metadata = message[1].decode("utf-8")
        # the metadata were received as string and have to be converted into
        # a dictionary
        metadata = json.loads(metadata)
        self.log.debug("Monitoring Client: %s", metadata)

        # TODO receive more than this one metadata unit
        event_message_list = [metadata]

        self.log.debug("event_message: %s", event_message_list)

        return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """

        if self.monitoringdevice is not None:
            self.monitoringdevice.terminate()
            self.monitoringdevice = None

        # close ZMQ
        self.stop_socket(name="mon_socket")

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
