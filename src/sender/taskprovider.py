from __future__ import unicode_literals

import socket
import zmq
import os
import logging
import json
import signal
import errno

from __init__ import BASE_PATH
from logutils.queue import QueueHandler
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


#
#  --------------------------  class: TaskProvider  ---------------------------
#

class TaskProvider():
    def __init__(self, config, control_con_id, request_fw_con_id,
                 router_con_id, log_queue, context=None):
        global BASE_PATH

        self.log = utils.get_logger("TaskProvider", log_queue)

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.current_pid = os.getpid()
        self.log.debug("TaskProvider started (PID {})."
                       .format(self.current_pid))

        self.eventdetector = None

        self.config = config

        self.control_con_id = control_con_id
        self.request_fw_con_id = request_fw_con_id
        self.router_con_id = router_con_id

        self.control_socket = None
        self.request_fw_socket = None
        self.router_socket = None

        self.poller = None

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
        self.eventdetector_m = __import__(self.config["event_detector_type"])

        self.eventdetector = self.eventdetector_m.EventDetector(self.config,
                                                                log_queue)

        self.continue_run = True

        try:
            self.create_sockets()

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

    def create_sockets(self):

        # socket to get control signals from
        try:
            self.control_socket = self.context.socket(zmq.SUB)
            self.control_socket.connect(self.control_con_id)
            self.log.info("Start control_socket (connect): '{}'"
                          .format(self.control_con_id))
        except:
            self.log.error("Failed to start control_socket (connect): '{}'"
                           .format(self.control_con_id), exc_info=True)
            raise

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")

        # socket to get requests
        try:
            self.request_fw_socket = self.context.socket(zmq.REQ)
            self.request_fw_socket.connect(self.request_fw_con_id)
            self.log.info("Start request_fw_socket (connect): '{}'"
                          .format(self.request_fw_con_id))
        except:
            self.log.error("Failed to start request_fw_socket (connect): '{}'"
                           .format(self.request_fw_con_id), exc_info=True)
            raise

        # socket to disribute the events to the worker
        try:
            self.router_socket = self.context.socket(zmq.PUSH)
            self.router_socket.bind(self.router_con_id)
            self.log.info("Start to router socket (bind): '{}'"
                          .format(self.router_con_id))
        except:
            self.log.error("Failed to start router Socket (bind): '{}'"
                           .format(self.router_con_id), exc_info=True)
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


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class RequestResponder():
    def __init__(self, request_fw_port, log_queue, context=None):
        # Send all logs to the main process
        self.log = utils.get_logger("RequestResponder", log_queue)

        self.context = context or zmq.Context.instance()
        self.request_fw_socket = self.context.socket(zmq.REP)
        connection_str = "tcp://127.0.0.1:{}".format(request_fw_port)
        self.request_fw_socket.bind(connection_str)
        self.log.info("[RequestResponder] request_fw_socket started (bind) "
                      "for '{}'".format(connection_str))

        self.run()

    def run(self):
        hostname = socket.getfqdn()
        self.log.info("[RequestResponder] Start run")
        open_requests = [['{}:6003'.format(hostname), 1, [".cbf"]],
                         ['{}:6004'.format(hostname), 0, [".cbf"]]]
        while True:
            request = self.request_fw_socket.recv_multipart()
            self.log.debug("[RequestResponder] Received request: {}"
                           .format(request))

            self.request_fw_socket.send(
                json.dumps(open_requests).encode("utf-8"))
            self.log.debug("[RequestResponder] Answer: {}"
                           .format(open_requests))

    def __exit__(self):
        self.request_fw_socket.close(0)
        self.context.destroy()


if __name__ == '__main__':
    from multiprocessing import Process, freeze_support, Queue
    import time
    from shutil import copyfile
    import setproctitle

    # see https://docs.python.org/2/library/multiprocessing.html#windows
    freeze_support()

    logfile = os.path.join(BASE_PATH, "logs", "taskprovider.log")
    logsize = 10485760

    config = {
        "event_detector_type": "inotifyx_events",
        "monitored_dir": os.path.join(BASE_PATH, "data", "source"),
        "fix_subdirs": ["commissioning", "current", "local"],
        "monitored_events": {"IN_CLOSE_WRITE": [".tif", ".cbf"],
                             "IN_MOVED_TO": [".log"]},
        "timeout": 0.1,
        "history_size": 0,
        "use_cleanup": False,
        "time_till_closed": 5,
        "action_time": 120
    }

    localhost = "127.0.0.1"

    control_port = "50005"
    request_fw_port = "6001"
    router_port = "7000"

    control_con_id = "tcp://{}:{}".format(localhost, control_port)
    request_fw_con_id = "tcp://{}:{}".format(localhost, request_fw_port)
    router_con_id = "tcp://{}:{}".format(localhost, router_port)

    setproctitle.setproctitle("taskprovider")

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = utils.get_log_handlers(logfile, logsize, verbose=True,
                                    onscreen_log_level="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = utils.CustomQueueListener(log_queue, h1, h2)
    log_queue_listener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    taskprovider_pr = Process(
        target=TaskProvider,
        args=(config, control_con_id, request_fw_con_id, router_con_id,
              log_queue))
    taskprovider_pr.start()

    requestResponderPr = Process(target=RequestResponder,
                                 args=(request_fw_port, log_queue))
    requestResponderPr.start()

    context = zmq.Context.instance()

    router_socket = context.socket(zmq.PULL)
    connection_str = "tcp://localhost:{}".format(router_port)
    router_socket.connect(connection_str)
    logging.info("=== router_socket connected to {}".format(connection_str))

    source_file = os.path.join(BASE_PATH, "test_file.cbf")
    target_file_base = os.path.join(
        BASE_PATH, "data", "source", "local", "raw") + os.sep
    if not os.path.exists(target_file_base):
        os.makedirs(target_file_base)

#    time.sleep(5)
    i = 100
    try:
        while i <= 105:
            time.sleep(0.5)
            target_file = "{}{}.cbf".format(target_file_base, i)
            logging.debug("copy to {}".format(target_file))
            copyfile(source_file, target_file)
#            call(["cp", source_file, target_file])
            i += 1

            workload = router_socket.recv_multipart()
            logging.info("=== next workload {}".format(workload))
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:

        requestResponderPr.terminate()
        taskprovider_pr.terminate()

        router_socket.close(0)
        context.destroy()

        for number in range(100, i):
            target_file = "{}{}.cbf".format(target_file_base, number)
            logging.debug("remove {}".format(target_file))
            os.remove(target_file)

        log_queue.put_nowait(None)
        log_queue_listener.stop()
