from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import zmq
import os
import json
import signal
import errno

import __init__ as init  # noqa F401
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TaskProvider():
    def __init__(self,
                 config,
                 control_con_str,
                 request_fw_con_str,
                 router_bind_str,
                 log_queue,
                 context=None):

        self.config = config
        self.control_con_str = control_con_str
        self.request_fw_con_str = request_fw_con_str
        self.router_bind_str = router_bind_str
        self.log_queue = log_queue

        self.log = None
        self.current_pid = None
        self.eventdetector = None

        self.control_socket = None
        self.request_fw_socket = None
        self.router_socket = None

        self.poller = None

        self.context = None
        self.ext_context = None
        self.eventdetector = None
        self.continue_run = None

        self.setup(context)

        try:
            self.run()
        except zmq.ZMQError:
            pass
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping TaskProvider due to unknown error "
                           "condition.", exc_info=True)
        finally:
            self.stop()

    def setup(self, context):
        self.log = utils.get_logger("TaskProvider", self.log_queue)

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.current_pid = os.getpid()
        self.log.debug("TaskProvider started (PID {})."
                       .format(self.current_pid))

        # remember if the context was created outside this class or not
        if context:
            self.context = context
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

        self.log.info("Loading event detector: {}"
                      .format(self.config["event_detector_type"]))
        eventdetector_m = __import__(self.config["event_detector_type"])

        self.eventdetector = eventdetector_m.EventDetector(self.config,
                                                           self.log_queue)

        self.continue_run = True

        try:
            self.create_sockets()
        except:
            self.log.error("Cannot create sockets", exc_info=True)
            self.stop()

    def create_sockets(self):

        # socket to get control signals from
        try:
            self.control_socket = self.context.socket(zmq.SUB)
            self.control_socket.connect(self.control_con_str)
            self.log.info("Start control_socket (connect): '{}'"
                          .format(self.control_con_str))
        except:
            self.log.error("Failed to start control_socket (connect): '{}'"
                           .format(self.control_con_str), exc_info=True)
            raise

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")

        # socket to get requests
        try:
            self.request_fw_socket = self.context.socket(zmq.REQ)
            self.request_fw_socket.connect(self.request_fw_con_str)
            self.log.info("Start request_fw_socket (connect): '{}'"
                          .format(self.request_fw_con_str))
        except:
            self.log.error("Failed to start request_fw_socket (connect): '{}'"
                           .format(self.request_fw_con_str), exc_info=True)
            raise

        # socket to disribute the events to the worker
        try:
            self.router_socket = self.context.socket(zmq.PUSH)
            self.router_socket.bind(self.router_bind_str)
            self.log.info("Start to router socket (bind): '{}'"
                          .format(self.router_bind_str))
        except:
            self.log.error("Failed to start router Socket (bind): '{}'"
                           .format(self.router_bind_str), exc_info=True)
            raise

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)

    def run(self):

        while self.continue_run:
            try:
                # the event for a file /tmp/test/source/local/file1.tif
                # is of the form:
                # {
                #   "source_path": "/tmp/test/source/"
                #   "relative_path": "local"
                #   "filename": "file1.tif"
                # }
                workload_list = self.eventdetector.get_new_event()
            except KeyboardInterrupt:
                break
            except IOError as e:
                if e.errno == errno.EINTR:
                    break
                else:
                    self.log.error("Invalid fileEvent message received.",
                                   exc_info=True)
                    workload_list = []
            except:
                self.log.error("Invalid fileEvent message received.",
                               exc_info=True)
                workload_list = []

            # TODO validate workload dict
            for workload in workload_list:
                # get requests for this event
                try:
                    self.log.debug("Get requests...")
                    self.request_fw_socket.send_multipart(
                        [b"GET_REQUESTS",
                         json.dumps(workload["filename"]).encode("utf-8")])

                    requests = json.loads(self.request_fw_socket.recv_string())
                    self.log.debug("Requests: {}".format(requests))
                except TypeError:
                    # This happens when CLOSE_FILE is sent as workload
                    requests = ["None"]
                except:
                    self.log.error("Get Requests... failed.", exc_info=True)
                    requests = ["None"]

                # build message dict
                try:
                    self.log.debug("Building message dict...")
                    # set correct escape characters
                    message_dict = json.dumps(workload).encode("utf-8")
                except:
                    self.log.error("Unable to assemble message dict.",
                                   exc_info=True)
                    continue

                # send the file to the dataDispatcher
                try:
                    self.log.debug("Sending message...")
                    message = [message_dict]
                    if requests != ["None"]:
                        message.append(json.dumps(requests).encode("utf-8"))
                    self.log.debug(str(message))
                    self.router_socket.send_multipart(message)
                except:
                    self.log.error("Sending message...failed.", exc_info=True)

            socks = dict(self.poller.poll(0))

            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):

                try:
                    message = self.control_socket.recv_multipart()
                    self.log.debug("Control signal received: message = {}"
                                   .format(message))
                except:
                    self.log.error("Waiting for control signal...failed",
                                   exc_info=True)
                    continue

                # remove subsription topic
                del message[0]

                if message[0] == b"EXIT":
                    self.log.debug("Requested to shutdown.")
                    break

                elif message[0] == b"SLEEP":
                    self.log.debug("Received sleep signal")
                    break_outer_loop = False

                    # if there are problems on the receiving side no data
                    # should be processed till the problem is solved
                    while True:
                        try:
                            message = self.control_socket.recv_multipart()
                        except:
                            self.log.error("Receiving control signal...failed")
                            continue

                        # remove subsription topic
                        del message[0]

                        if message[0] == b"SLEEP":
                            self.log.debug("Received sleep signal")
                            continue
                        elif message[0] == b"WAKEUP":
                            self.log.debug("Received wakeup signal")
                            # Wake up from sleeping
                            break
                        elif message[0] == b"EXIT":
                            self.log.debug("Received exit signal")
                            break_outer_loop = True
                            break
                        else:
                            self.log.error("Unhandled control signal received:"
                                           " {}".format(message))

                    # the exit signal should become effective
                    if break_outer_loop:
                        break
                    else:
                        continue

                elif message[0] == b"WAKEUP":
                    self.log.debug("Received wakeup signal without sleeping. "
                                   "Do nothing.")
                    continue

                else:
                    self.log.error("Unhandled control signal received: {}"
                                   .format(message))

    def stop(self):
        self.continue_run = False

        self.log.debug("Closing sockets for TaskProvider")
        if self.router_socket:
            self.log.info("Closing router_socket")
            self.router_socket.close(0)
            self.router_socket = None

        if self.request_fw_socket:
            self.log.info("Closing request_fw_socket")
            self.request_fw_socket.close(0)
            self.request_fw_socket = None

        if self.control_socket:
            self.log.info("Closing control_socket")
            self.control_socket.close(0)
            self.control_socket = None

        if not self.ext_context and self.context:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

    def signal_term_handler(self, signal, frame):
        self.log.debug('got SIGTERM')
        self.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()

# testing was moved into test/unittests/core/test_taskprovider.py
