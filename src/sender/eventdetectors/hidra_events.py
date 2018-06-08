from __future__ import print_function
from __future__ import unicode_literals

import os
import json
import tempfile
import zmq
# from zmq.devices.monitoredqueuedevice import ThreadMonitoredQueue
from zmq.utils.strtypes import asbytes
import multiprocessing

from eventdetectorbase import EventDetectorBase
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class MonitorDevice():
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
#                print ("[MonitoringDevice] In: Received message {0}"
#                        .format(msg[0][:20]))
            except KeyboardInterrupt:
                break

            if msg != [b'ALIVE_TEST']:

                try:
                    mon_msg = [self.in_prefix] + msg
                    self.mon_socket.send_multipart(mon_msg)
#                    print ("[MonitoringDevice] Mon: Sent message")

                    self.out_socket.send_multipart(msg)
#                    print ("[MonitoringDevice] Out: Sent message {0}"
#                            .format([msg[0], msg[1][:20]]))

                    mon_msg = [self.out_prefix] + msg
                    self.mon_socket.send_multipart(mon_msg)
#                    print ("[MonitoringDevice] Mon: Sent message")
                except KeyboardInterrupt:
                    break


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self, config, log_queue,
                                   "hidra_events")

        if utils.is_windows():
            required_params = ["context",
                               "ext_ip",
                               "event_det_port",
                               "ext_data_port"
                               "data_fetch_port"]
        else:
            required_params = ["context",
                               "ext_ip",
                               "ipc_path",
                               "main_pid",
                               "ext_data_port"]

        # Check format of config
        check_passed, config_reduced = utils.check_config(required_params,
                                                          config,
                                                          self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {}"
                          .format(config_reduced))

            if utils.is_windows():
                self.in_con_str = ("tcp://{}:{}"
                                   .format(config["ext_ip"],
                                           config["ext_data_port"]))
                self.out_con_str = ("tcp://{}:{}"
                                    .format(config["ext_ip"],
                                            config["data_fetcher_port"]))
                self.mon_con_str = ("tcp://{}:{}"
                                    .format(config["ext_ip"],
                                            config["event_det_port"]))
            else:
                self.in_con_str = ("tcp://{}:{}"
                                   .format(config["ext_ip"],
                                           config["ext_data_port"]))
                self.out_con_str = ("ipc://{}/{}_{}"
                                    .format(config["ipc_path"],
                                            config["main_pid"],
                                            "out"))
                self.mon_con_str = ("ipc://{}/{}_{}"
                                    .format(config["ipc_path"],
                                            config["main_pid"],
                                            "mon"))

        else:
            # self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

        # Set up monitored queue to get notification when new data is sent to
        # the zmq queue

        self.monitoringdevice = multiprocessing.Process(
            target=MonitorDevice,
            args=(self.in_con_str,
                  self.out_con_str,
                  self.mon_con_str))

        """ original monitored queue from pyzmq is not working
        in_prefix = asbytes('in')
        out_prefix = asbytes('out')

        self.monitoringdevice = ThreadMonitoredQueue(
            #   in       out      mon
            zmq.PULL, zmq.PUSH, zmq.PUB, in_prefix, out_prefix)

        self.monitoringdevice.bind_in(self.in_con_str)
        self.monitoringdevice.bind_out(self.out_con_str)
        self.monitoringdevice.bind_mon(self.mon_con_str)
        """

        self.monitoringdevice.start()
        self.log.info("Monitoring device has started with (bind)\n"
                      "in: {}\nout: {}\nmon: {}"
                      .format(self.in_con_str,
                              self.out_con_str,
                              self.mon_con_str))

        # set up monitoring socket where the events are sent to
        if config["context"] is not None:
            self.context = config["context"]
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

        self.mon_socket = None

        self.create_sockets()

    def create_sockets(self):

        # Create zmq socket to get events
        try:
            self.mon_socket = self.context.socket(zmq.PULL)
            self.mon_socket.connect(self.mon_con_str)
#            self.mon_socket.setsockopt_string(zmq.SUBSCRIBE, "")

            self.log.info("Start monitoring socket (connect): '{}'"
                          .format(self.mon_con_str))
        except:
            self.log.error("Failed to start monitoring socket (connect): '{}'"
                           .format(self.mon_con_str), exc_info=True)
            raise

    def get_new_event(self):

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

        self.monitoringdevice.terminate()

        # close ZMQ
        if self.mon_socket:
            self.log.info("Closing mon_socket")
            self.mon_socket.close(0)
            self.mon_socket = None

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.ext_context and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)

# testing was moved into test/unittests/event_detectors/test_hidra_events.py
