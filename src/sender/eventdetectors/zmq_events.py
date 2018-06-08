from __future__ import print_function
from __future__ import unicode_literals

import os
import zmq
import json

from eventdetectorbase import EventDetectorBase
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self, config, log_queue,
                                   "zmq_events")

        if utils.is_windows():
            required_params = ["context",
                               "number_of_streams",
                               "ext_ip",
                               "event_det_port"]
        else:
            required_params = ["context",
                               "number_of_streams",
                               "ipc_path",
                               "main_pid"]

        # Check format of config
        check_passed, config_reduced = utils.check_config(required_params,
                                                          config,
                                                          self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {}"
                          .format(config_reduced))

            if utils.is_windows():
                self.event_det_con_str = ("tcp://{}:{}"
                                          .format(config["ext_ip"],
                                                  config["event_det_port"]))
            else:
                self.event_det_con_str = ("ipc://{}/{}_{}"
                                          .format(config["ipc_path"],
                                                  config["main_pid"],
                                                  "eventDet"))

            self.event_socket = None

            self.number_of_streams = config["number_of_streams"]

            # remember if the context was created outside this class or not
            if config["context"]:
                self.context = config["context"]
                self.ext_context = True
            else:
                self.log.info("Registering ZMQ context")
                self.context = zmq.Context()
                self.ext_context = False

            self.create_sockets()

        else:
            # self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    def create_sockets(self):

        # Create zmq socket to get events
        try:
            self.event_socket = self.context.socket(zmq.PULL)
            self.event_socket.bind(self.event_det_con_str)
            self.log.info("Start event_socket (bind): '{}'"
                          .format(self.event_det_con_str))
        except:
            self.log.error("Failed to start event_socket (bind): '{}'"
                           .format(self.event_det_con_str), exc_info=True)
            raise

    def get_new_event(self):

        event_message = self.event_socket.recv_multipart()

        if event_message[0] == b"CLOSE_FILE":
            event_message_list = [event_message
                                  for i in range(self.number_of_streams)]
        else:
            event_message_list = [json.loads(event_message[0].decode("utf-8"))]

        self.log.debug("event_message: {}".format(event_message_list))

        return event_message_list

    def stop(self):
        # close ZMQ
        if self.event_socket:
            self.event_socket.close(0)
            self.event_socket = None

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

# testing was moved into test/unittests/event_detectors/test_zmq_events
