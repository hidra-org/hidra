from __future__ import print_function
from __future__ import unicode_literals

import os
import zmq
import logging
import json
import tempfile

from logutils.queue import QueueHandler

from __init__ import BASE_PATH
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetector():

    def __init__(self, config, log_queue):

        self.log = helpers.get_logger("zmq_events", log_queue)

        if helpers.is_windows():
            required_params = ["context",
                               "number_of_streams",
                               "ext_ip",
                               "event_det_port"]
        else:
            required_params = ["context",
                               "number_of_streams",
                               "ipc_path"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            config,
                                                            self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {0}"
                          .format(config_reduced))

            if helpers.is_windows():
                self.event_det_con_str = ("tcp://{0}:{1}"
                                          .format(config["ext_ip"],
                                                  config["event_det_port"]))
            else:
                self.event_det_con_str = ("ipc://{0}/{1}"
                                          .format(config["ipc_path"],
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
            self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    def create_sockets(self):

        # Create zmq socket to get events
        try:
            self.event_socket = self.context.socket(zmq.PULL)
            self.event_socket.bind(self.event_det_con_str)
            self.log.info("Start event_socket (bind): '{0}'"
                          .format(self.event_det_con_str))
        except:
            self.log.error("Failed to start event_socket (bind): '{0}'"
                           .format(self.event_det_con_str), exc_info=True)
            raise

    def get_new_event(self):

        event_message = self.event_socket.recv_multipart()

        if event_message[0] == b"CLOSE_FILE":
            event_message_list = [event_message
                                  for i in range(self.number_of_streams)]
        else:
            event_message_list = [json.loads(event_message[0].decode("utf-8"))]

        self.log.debug("event_message: {0}".format(event_message_list))

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

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    import time
    from multiprocessing import Queue

    logfile = os.path.join(BASE_PATH, "logs", "zmqDetector.log")
    logsize = 10485760

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True,
                                      onscreen_log_level="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = helpers.CustomQueueListener(log_queue, h1, h2)
    log_queue_listener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    ipc_path = os.path.join(tempfile.gettempdir(), "hidra")

    event_det_con_str = "ipc://{0}/{1}".format(ipc_path, "eventDet")
    print ("event_det_con_str", event_det_con_str)
    number_of_streams = 1
    config = {
        "context": None,
        "number_of_streams": number_of_streams,
        "ext_ip": "0.0.0.0",
        "event_det_port": 50003,
        "ipc_path": ipc_path
    }

    eventdetector = EventDetector(config, log_queue)

    source_file = os.path.join(BASE_PATH, "test_file.cbf")
    target_file_base = os.path.join(
        BASE_PATH, "data", "source", "local", "raw") + os.sep

    context = zmq.Context.instance()

    # create zmq socket to send events
    event_socket = context.socket(zmq.PUSH)
    event_socket.connect(event_det_con_str)
    logging.info("Start event_socket (connect): '{0}'"
                 .format(event_det_con_str))

    i = 100
    while i <= 101:
        try:
            logging.debug("generate event")
            target_file = "{0}{1}.cbf".format(target_file_base, i)
            message = {
                "filename": target_file,
                "filepart": 0,
                "chunksize": 10
            }

            event_socket.send_multipart([json.dumps(message).encode("utf-8")])
            i += 1

            event_list = eventdetector.get_new_event()
            if event_list:
                logging.debug("event_list: {0}".format(event_list))

            time.sleep(1)
        except KeyboardInterrupt:
            break

    event_socket.send_multipart(
        [b"CLOSE_FILE", "test_file.cbf".encode("utf8")])

    event_list = eventdetector.get_new_event()
    logging.debug("event_list: {0}".format(event_list))

    log_queue.put_nowait(None)
    log_queue_listener.stop()
