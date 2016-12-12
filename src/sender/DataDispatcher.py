from __future__ import unicode_literals

import zmq
import os
import time
import logging
import json
import signal
from multiprocessing import Process

from logutils.queue import QueueHandler
from __init__ import BASE_PATH
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


#
# --------------------------  class: DataDispatcher  --------------------------
#
class DataDispatcher():

    def __init__(self, id, control_con_id, router_con_id, chunksize,
                 fixed_stream_id, config, log_queue,
                 local_target=None, context=None):

        self.id = id
        self.log = self.__get_logger(log_queue)

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        self.current_pid = os.getpid()
        self.log.debug("DataDispatcher-{0} started (PID {1})."
                       .format(self.id, self.current_pid))

        self.control_con_id = control_con_id
        self.router_con_id = router_con_id

        self.control_socket = None
        self.router_socket = None

        self.poller = None

        self.chunksize = chunksize

        self.fixed_stream_id = fixed_stream_id
        self.local_target = local_target

        self.config = config
        self.log.info("Configuration for data fetcher: {0}"
                      .format(self.config))

        datafetcher = self.config["data_fetcher_type"]

        # dict with information of all open sockets to which a data stream is
        # opened (host, port,...)
        self.open_connections = dict()

        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False
            if ("context" in self.config
                    and not self.config["context"]):
                self.config["context"] = self.context

        self.log.info("Loading data fetcher: {0}".format(datafetcher))
        self.datafetcher = __import__(datafetcher)

        self.continue_run = True

        if (self.datafetcher.setup(self.log, config)):
            try:
                self.__create_sockets()

                self.run()
            except zmq.ZMQError:
                pass
            except KeyboardInterrupt:
                pass
            except:
                self.log.error("Stopping DataDispatcher-{0} due to unknown "
                               "error condition.".format(self.id),
                               exc_info=True)
            finally:
                self.stop()

    def __create_sockets(self):

        # socket for control signals
        try:
            self.control_socket = self.context.socket(zmq.SUB)
            self.control_socket.connect(self.control_con_id)
            self.log.info("Start control_socket (connect): '{0}'"
                          .format(self.control_con_id))
        except:
            self.log.error("Failed to start control_socket (connect): '{0}'"
                           .format(self.control_con_id), exc_info=True)
            raise

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "signal")

        # socket to get new workloads from
        try:
            self.router_socket = self.context.socket(zmq.PULL)
            self.router_socket.connect(self.router_con_id)
            self.log.info("Start router_socket (connect): '{0}'"
                          .format(self.router_con_id))
        except:
            self.log.error("Failed to start router_socket (connect): '{0}'"
                           .format(self.router_con_id), exc_info=True)
            raise

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)
        self.poller.register(self.router_socket, zmq.POLLIN)

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def __get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("DataDispatcher-{0}".format(self.id))
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def run(self):

        fixed_stream_id = [self.fixed_stream_id, 0, [""], "data"]

        while self.continue_run:
            self.log.debug("DataDispatcher-{0}: waiting for new job"
                           .format(self.id))
            socks = dict(self.poller.poll())

            ######################################
            #     messages from TaskProvider     #
            ######################################
            if (self.router_socket in socks
                    and socks[self.router_socket] == zmq.POLLIN):

                try:
                    message = self.router_socket.recv_multipart()
                    self.log.debug("DataDispatcher-{0}: new job received"
                                   .format(self.id))
                    self.log.debug("message = {0}".format(message))
                except:
                    self.log.error("DataDispatcher-{0}: waiting for new job"
                                   "...failed".format(self.id), exc_info=True)
                    continue

                if len(message) >= 2:

                    workload = json.loads(message[0].decode("utf-8"))
                    targets = json.loads(message[1].decode("utf-8"))

                    if self.fixed_stream_id:
                        targets.insert(0, fixed_stream_id)
                        self.log.debug("Added fixed_stream_id {0} to targets "
                                       "{1}".format(fixed_stream_id, targets))

                    # sort the target list by the priority
                    targets = sorted(targets, key=lambda target: target[1])

                else:
                    workload = json.loads(message[0].decode("utf-8"))

                    if type(workload) == list and workload[0] == b"CLOSE_FILE":

                        # woraround for error
                        # "TypeError: Frame 0 (u'CLOSE_FILE') does not support
                        # the buffer interface."
                        workload[0] = b"CLOSE_FILE"
                        for i in range(1, len(workload)):
                            workload[i] = (json.dumps(workload[i])
                                           .encode("utf-8")
                                           )

                        if self.fixed_stream_id:
                            self.log.debug("Router requested to send signal "
                                           "that file was closed.")
                            workload.append(self.id)

                            # socket already known
                            if self.fixed_stream_id in self.open_connections:
                                tracker = (
                                    self.open_connections[self.fixed_stream_id]
                                    .send_multipart(workload,
                                                    copy=False,
                                                    track=True)
                                    )
                                self.log.info("Sending close file signal to "
                                              "'{0}' with priority 0"
                                              .format(self.fixed_stream_id))
                            else:
                                # open socket
                                socket = self.context.socket(zmq.PUSH)
                                connection_str = "tcp://{0}".format(
                                    self.fixed_stream_id)

                                socket.connect(connection_str)
                                self.log.info("Start socket (connect): '{0}'"
                                              .format(connection_str))

                                # register socket
                                self.open_connections[self.fixed_stream_id] = (
                                    socket
                                    )

                                # send data
                                tracker = (
                                    self.open_connections[self.fixed_stream_id]
                                    .send_multipart(workload,
                                                    copy=False,
                                                    track=True)
                                    )
                                self.log.info("Sending close file signal to "
                                              "'{0}' with priority 0"
                                              .format(fixed_stream_id))

                            # socket not known
                            if not tracker.done:
                                self.log.info("Close file signal has not "
                                              "been sent yet, waiting...")
                                tracker.wait()
                                self.log.info("Close file signal has not "
                                              "been sent yet, waiting...done")

                            time.sleep(2)
                            self.log.debug("Continue after sleeping")
                            continue
                        else:
                            self.log.warning("Router requested to send signal"
                                             "that file was closed, but no "
                                             "target specified")
                            continue

                    elif self.fixed_stream_id:
                        targets = [fixed_stream_id]
                        self.log.debug("Added fixed_stream_id to targets {0}."
                                       .format(targets))

                    else:
                        targets = []

                # get metadata and paths of the file
                try:
                    self.log.debug("Getting file paths and metadata")
                    source_file, target_file, metadata = (
                        self.datafetcher.get_metadata(
                            self.log, self.config, targets, workload,
                            self.chunksize, self.local_target)
                        )

                except:
                    self.log.error("Building of metadata dictionary failed "
                                   "for workload: {0}".format(workload),
                                   exc_info=True)
                    # skip all further instructions and
                    # continue with next iteration
                    continue

                # send data
                try:
                    self.datafetcher.send_data(self.log, targets, source_file,
                                               target_file, metadata,
                                               self.open_connections,
                                               self.context,
                                               self.config)
                except:
                    self.log.error("DataDispatcher-{0}: Passing new file to "
                                   "data stream...failed".format(self.id),
                                   exc_info=True)

                # finish data handling
                self.datafetcher.finish_datahandling(self.log, targets,
                                                     source_file, target_file,
                                                     metadata,
                                                     self.open_connections,
                                                     self.context,
                                                     self.config)

            ######################################
            #         control commands           #
            ######################################
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):

                try:
                    message = self.control_socket.recv_multipart()
                    self.log.debug("DataDispatcher-{0}: control signal "
                                   "received".format(self.id))
                    self.log.debug("message = {0}".format(message))
                except:
                    self.log.error("DataDispatcher-{0}: waiting for control "
                                   "signal...failed".format(self.id),
                                   exc_info=True)
                    continue

                # remove subsription topic
                del message[0]

                if message[0] == b"EXIT":
                    self.log.debug("Router requested to shutdown "
                                   "DataDispatcher-{0}.".format(self.id))
                    break

                elif message[0] == b"CLOSE_SOCKETS":

                    targets = json.loads(message[1].decode("utf-8"))

                    for socket_id, prio, suffix in targets:
                        if socket_id in self.open_connections:
                            self.log.info("Closing socket {0}".format(
                                socket_id))
                            if self.open_connections[socket_id]:
                                self.open_connections[socket_id].close(0)
                            del self.open_connections[socket_id]
                    continue
                else:
                    self.log.error("Unhandled control signal received: {0}"
                                   .format(message))

    def stop(self):
        self.continue_run = False
        self.log.debug("Closing sockets for DataDispatcher-{0}"
                       .format(self.id))

        for connection in self.open_connections:
            if self.open_connections[connection]:
                self.log.info("Closing socket {0}".format(connection))
                self.open_connections[connection].close(0)
                self.open_connections[connection] = None

        if self.control_socket:
            self.log.info("Closing control_socket")
            self.control_socket.close(0)
            self.control_socket = None

        if self.router_socket:
            self.log.info("Closing router_socket")
            self.router_socket.close(0)
            self.router_socket = None

        self.datafetcher.clean(self.config)

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


if __name__ == '__main__':
    from multiprocessing import freeze_support, Queue
    from shutil import copyfile

    # see https://docs.python.org/2/library/multiprocessing.html#windows
    freeze_support()

    logfile = os.path.join(BASE_PATH, "logs", "datadispatcher.log")
    logsize = 10485760

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize,
                                      verbose=True,
                                      onscreen_log_level="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = helpers.CustomQueueListener(log_queue, h1, h2)
    log_queue_listener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    source_file = os.path.join(BASE_PATH, "test_file.cbf")
    target_file = os.path.join(BASE_PATH, "data", "source", "local", "100.cbf")

    copyfile(source_file, target_file)
    time.sleep(0.5)

    localhost = "127.0.0.1"
    control_port = "50005"
    router_port = "7000"

    control_con_id = "tcp://{0}:{1}".format(localhost, control_port)
    router_con_id = "tcp://{0}:{1}".format(localhost, router_port)

    receiving_port = "6005"
    receiving_port2 = "6006"

    chunksize = 10485760  # = 1024*1024*10 = 10 MiB

    local_target = os.path.join(BASE_PATH, "data", "target")
    fixed_stream_id = False
    fixed_stream_id = "localhost:6006"

    config = {
        "data_fetcher_type": "file_fetcher",
        "fix_subdirs": ["commissioning", "current", "local"],
        "store_data": False,
        "remove_data": False
        }

    context = zmq.Context.instance()

    datadispatcher_pr = Process(target=DataDispatcher,
                                args=(1, control_con_id, router_con_id,
                                      chunksize, fixed_stream_id, config,
                                      log_queue, local_target, context))
    datadispatcher_pr.start()

    router_socket = context.socket(zmq.PUSH)
    connection_str = "tcp://127.0.0.1:{0}".format(router_port)
    router_socket.bind(connection_str)
    logging.info("=== router_socket connected to {0}".format(connection_str))

    receiving_socket = context.socket(zmq.PULL)
    connection_str = "tcp://0.0.0.0:{0}".format(receiving_port)
    receiving_socket.bind(connection_str)
    logging.info("=== receiving_socket connected to {0}"
                 .format(connection_str))

    receiving_socket2 = context.socket(zmq.PULL)
    connection_str = "tcp://0.0.0.0:{0}".format(receiving_port2)
    receiving_socket2.bind(connection_str)
    logging.info("=== receiving_socket2 connected to {0}"
                 .format(connection_str))

    metadata = {
        "source_path": os.path.join(BASE_PATH, "data", "source"),
        "relative_path": "local",
        "filename": "100.cbf"
        }
    targets = [['localhost:6005', 1, [".cbf"], "data"],
               ['localhost:6006', 0, [".cbf"], "data"]]

    message = [json.dumps(metadata).encode("utf-8"),
               json.dumps(targets).encode("utf-8")]
#    message = [json.dumps(metadata).encode("utf-8")]

    time.sleep(1)

    router_socket.send_multipart(message)
    logging.info("=== send message")

    try:
        recv_message = receiving_socket.recv_multipart()
        logging.info("=== received: {0}"
                     .format(json.loads(recv_message[0].decode("utf-8"))))
        recv_message = receiving_socket2.recv_multipart()
        logging.info("=== received 2: {0}"
                     .format(json.loads(recv_message[0].decode("utf-8"))))
    except KeyboardInterrupt:
        pass
    finally:
        datadispatcher_pr.terminate()

        router_socket.close(0)
        receiving_socket.close(0)
        receiving_socket2.close(0)
        context.destroy()

        log_queue.put_nowait(None)
        log_queue_listener.stop()
