from __future__ import print_function
from __future__ import unicode_literals

import os
import json
import tempfile
import zmq
#from zmq.devices.monitoredqueuedevice import ThreadMonitoredQueue
from zmq.utils.strtypes import asbytes
import multiprocessing

from eventdetectorbase import EventDetectorBase
import helpers

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
#                       .format(msg[:20]))
            except KeyboardInterrupt:
                break

            if msg != [b'ALIVE_TEST']:

                mon_msg = [self.in_prefix] + msg
                self.mon_socket.send_multipart(mon_msg)
#                print ("[MonitoringDevice] Mon: Sent message")

                self.out_socket.send_multipart(msg)
#                print ("[MonitoringDevice] Out: Sent message {0}"
#                       .format(msg[:20]))

                mon_msg = [self.out_prefix] + msg
                self.mon_socket.send_multipart(mon_msg)
#                print ("[MonitoringDevice] Mon: Sent message")


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self, config, log_queue,
                                   "hidra_events")

        if helpers.is_windows():
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
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            config,
                                                            self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {0}"
                          .format(config_reduced))

            if helpers.is_windows():
                self.in_con_str = ("tcp://{0}:{1}"
                                   .format(config["ext_ip"],
                                           config["ext_data_port"]))
                self.out_con_str = ("tcp://{0}:{1}"
                                    .format(config["ext_ip"],
                                            config["data_fetcher_port"]))
                self.mon_con_str = ("tcp://{0}:{1}"
                                    .format(config["ext_ip"],
                                            config["event_det_port"]))
            else:
                self.in_con_str = ("tcp://{0}:{1}"
                                   .format(config["ext_ip"],
                                           config["ext_data_port"]))
                self.out_con_str = ("ipc://{0}/{1}_{2}"
                                    .format(config["ipc_path"],
                                            config["main_pid"],
                                            "out"))
                self.mon_con_str = ("ipc://{0}/{1}_{2}"
                                    .format(config["ipc_path"],
                                            config["main_pid"],
                                            "mon"))

        else:
            self.log.debug("config={0}".format(config))
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
                      "in: {0}\nout: {1}\nmon: {2}"
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

        self.create_sockets()

    def create_sockets(self):

        # Create zmq socket to get events
        try:
            self.mon_socket = self.context.socket(zmq.PULL)
            self.mon_socket.connect(self.mon_con_str)
#            self.mon_socket.setsockopt_string(zmq.SUBSCRIBE, "")

            self.log.info("Start monitoring socket (connect): '{0}'"
                          .format(self.mon_con_str))
        except:
            self.log.error("Failed to start monitoring socket (connect): '{0}'"
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
        self.log.debug("Monitoring Client: {0}".format(metadata))

        # TODO receive more than this one metadata unit
        event_message_list = [metadata]

        self.log.debug("event_message: {0}".format(event_message_list))

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


if __name__ == '__main__':
    from multiprocessing import Queue
    from logutils.queue import QueueHandler
    from __init__ import BASE_PATH
    import logging

    logfile = os.path.join(BASE_PATH, "logs", "hidra_events.log")
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
#    current_pid = os.getpid()
    current_pid = 12345

    context = zmq.Context()

    config = {
        "context": context,
        "ext_ip": "131.169.185.121",
        "ipc_path": ipc_path,
        "main_pid": current_pid,
        "ext_data_port": "50100"
    }

    if not os.path.exists(ipc_path):
        os.mkdir(ipc_path)
        os.chmod(ipc_path, 0o777)
        logging.info("Creating directory for IPC communication: {0}"
                     .format(ipc_path))

    eventdetector = EventDetector(config, log_queue)

    source_file = os.path.join(BASE_PATH, "test_file.cbf")
    target_file_base = os.path.join(
        BASE_PATH, "data", "source", "local") + os.sep

    in_con_str = "tcp://{0}:{1}".format(config["ext_ip"],
                                        config["ext_data_port"])
    out_con_str = "ipc://{0}/{1}_{2}".format(ipc_path, current_pid, "out")

    local_in = True
    local_out = True

    if local_in:
        # create zmq socket to send events
        data_in_socket = context.socket(zmq.PUSH)
        data_in_socket.connect(in_con_str)
        logging.info("Start data_in_socket (connect): '{0}'"
                     .format(in_con_str))

    if local_out:
        data_out_socket = context.socket(zmq.PULL)
        data_out_socket.connect(out_con_str)
        logging.info("Start data_out_socket (connect): '{0}'"
                     .format(out_con_str))

    i = 100
    try:
        while i <= 101:
            logging.debug("generate event")
            target_file = "{0}{1}.cbf".format(target_file_base, i)
            message = {
                "filename": target_file,
                "filepart": 0,
                "chunksize": 10
            }

            if local_in:
                data_in_socket.send_multipart(
                    [json.dumps(message).encode("utf-8"), b"incoming_data"])

            i += 1
            event_list = eventdetector.get_new_event()
            if event_list:
                logging.debug("event_list: {0}".format(event_list))

            if local_out:
                message = data_out_socket.recv_multipart()
                logging.debug("Received - {0}".format(message))
    except KeyboardInterrupt:
        pass
    finally:
        eventdetector.stop()
        if local_in:
            data_in_socket.close()
        if local_out:
            data_out_socket.close()
        context.destroy()

        log_queue.put_nowait(None)
        log_queue_listener.stop()
