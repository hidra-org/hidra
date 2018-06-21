from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

from collections import namedtuple
import json
import zmq
# from zmq.devices.monitoredqueuedevice import ThreadMonitoredQueue
from zmq.utils.strtypes import asbytes
import multiprocessing

from eventdetectorbase import EventDetectorBase
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


IpcEndpoints = namedtuple("ipc_endpoints", ["out", "mon"])
Addresses = namedtuple("addresses", ["in_bind", "in_con",
                                     "out_bind", "out_con",
                                     "mon_bind", "mon_con"])


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

        out = "{}_{}".format(ipc_ip, "out")
        mon = "{}_{}".format(ipc_ip, "mon")

        endpoints = IpcEndpoints(out=out, mon=mon)

    return endpoints


def get_addrs(config, ipc_endpoints):
    """Configures the ZMQ address depending on the protocol.

    Args:
        config (dict): A dictionary containing the IPs to bind and to connect
                       to as well as the ports. Usually con_ip is teh DNS name.
        ipc_endpoints: The endpoints used for the interprocess communication
                       (ipc) protocol.
    Returns:
        An Addresses object containing the bind and connection addresses.
    """

    ext_ip = config["ext_ip"]
    con_ip = config["con_ip"]

    port = config["ext_data_port"]
    in_bind = "tcp://{}:{}".format(ext_ip, port)
    in_con = "tcp://{}:{}".format(con_ip, port)

    if utils.is_windows():
        port = config["data_fetcher_port"]
        out_bind = "tcp://{}:{}".format(ext_ip, port)
        out_con = "tcp://{}:{}".format(con_ip, port)

        port = config["event_det_port"]
        mon_bind = "tcp://{}:{}".format(ext_ip, port)
        mon_con = "tcp://{}:{}".format(con_ip, port)
    else:
        out_bind = "ipc://{}".format(ipc_endpoints.out)
        out_con = out_bind

        mon_bind = "ipc://{}".format(ipc_endpoints.mon)
        mon_con = mon_bind

    return Addresses(
        in_bind=in_bind, in_con=in_con,
        out_bind=out_bind, out_con=out_con,
        mon_bind=mon_bind, mon_con=mon_con
    )


class MonitorDevice(object):
    def __init__(self, in_con_str, out_con_str, mon_con_str):

        self.in_prefix = asbytes('in')
        self.out_prefix = asbytes('out')

        self.context = zmq.Context()

        self.in_socket = self.context.socket(zmq.PULL)
        self.in_socket.bind(in_con_str)

        self.out_socket = self.context.socket(zmq.PUSH)
        self.out_socket.bind(out_con_str)

        self.mon_socket = self.context.socket(zmq.PUSH)
        self.mon_socket.bind(mon_con_str)

        self.run()

    def run(self):
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
    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config,
                                   log_queue,
                                   "hidra_events")

        self.config = config
        self.log_queue = log_queue

        self.ipc_endpoints = None
        self.addrs = None

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

        self.required_params = ["context", "ext_ip", "con_ip", "ext_data_port"]
        if utils.is_windows():
            self.required_params += ["event_det_port", "data_fetch_port"]
        else:
            self.required_params += ["ipc_dir", "main_pid"]

    def setup(self):
        """Configures ZMQ sockets and starts monitoring device.
        """

        self.ipc_endpoints = get_ipc_endpoints(config=self.config)
        self.addrs = get_addrs(config=self.config,
                               ipc_endpoints=self.ipc_endpoints)

        # Set up monitored queue to get notification when new data is sent to
        # the zmq queue

        self.monitoringdevice = multiprocessing.Process(
            target=MonitorDevice,
            args=(self.addrs.in_bind,
                  self.addrs.out_bind,
                  self.addrs.mon_bind)
        )

        # original monitored queue from pyzmq is not working
        # > in_prefix = asbytes('in')
        # > out_prefix = asbytes('out')
        #
        # > self.monitoringdevice = ThreadMonitoredQueue(
        # >   #   in       out      mon
        # >   zmq.PULL, zmq.PUSH, zmq.PUB, in_prefix, out_prefix)
        #
        # > self.monitoringdevice.bind_in(self.addrs.in_bind)
        # > self.monitoringdevice.bind_out(self.addrs.out_bind)
        # > self.monitoringdevice.bind_mon(self.addrs.mon_bind)

        self.monitoringdevice.start()
        self.log.info("Monitoring device has started with (bind)\n"
                      "in: {}\nout: {}\nmon: {}"
                      .format(self.addrs.in_bind,
                              self.addrs.out_bind,
                              self.addrs.mon_bind))

        # set up monitoring socket where the events are sent to
        if self.config["context"] is not None:
            self.context = self.config["context"]
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

        # Create zmq socket to get events
        try:
            self.mon_socket = self.context.socket(zmq.PULL)
            self.mon_socket.connect(self.addrs.mon_con)
#            self.mon_socket.setsockopt_string(zmq.SUBSCRIBE, "")

            self.log.info("Start monitoring socket (connect): '{}'"
                          .format(self.addrs.mon_con))
        except:
            self.log.error("Failed to start monitoring socket (connect): '{}'"
                           .format(self.addrs.mon_con), exc_info=True)
            raise

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
        self.log.debug("Monitoring Client: {}".format(metadata))

        # TODO receive more than this one metadata unit
        event_message_list = [metadata]

        self.log.debug("event_message: {}".format(event_message_list))

        return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """

        if self.monitoringdevice is not None:
            self.monitoringdevice.terminate()
            self.monitoringdevice = None

        # close ZMQ
        if self.mon_socket is not None:
            self.log.info("Closing mon_socket")
            self.mon_socket.close(0)
            self.mon_socket = None

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
