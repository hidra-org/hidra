from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import time

import msgpack
import msgpack_numpy
msgpack_numpy.patch()

from datafetcherbase import DataFetcherBase
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id, context):

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "xfel_fetcher-{}".format(id),
                                 context)

        required_params = ["ipc_path",
                           "main_pid"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            self.config,
                                                            self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {}"
                          .format(config_reduced))

            self.config = config
            self.socket = None

            self.data_fetcher_con_str = ("ipc://{}/{}_{}"
                                         .format(config["ipc_path"],
                                                 config["main_pid"],
                                                 "data_fetcher"))

        else:
            # self.log.debug("config={0}".format(self.config))
            raise Exception("Wrong configuration")

        self.create_sockets()

    def create_sockets(self):
        # Create zmq socket
        try:
            self.log.info("Start datafetcher socket...")
            self.socket = self.context.socket(zmq.PULL)
            self.socket.connect(self.data_fetcher_con_str)
            self.log.info("Start datafetcher socket (connect): '{}'"
                          .format(self.data_fetcher_con_str))
        except:
            self.log.error("Failed to start com_socket (connect): '{}'"
                           .format(data_fetcher_con_str), exc_info=True)
            raise

    def get_metadata(self, targets, metadata):

        # extract event metadata
        try:
            # TODO validate metadata dict
            self.source_file = metadata["filename"]
        except:
            self.log.error("Invalid fileEvent message received.",
                           exc_info=True)
            self.log.debug("metadata={}".format(metadata))
            # skip all further instructions and continue with next iteration
            raise

        if targets:
            try:
                self.log.debug("create metadata for source file...")
                # metadata = {
                #        "filename"       : ...,
                #        "file_mod_time"    : ...,
                #        "file_create_time" : ...,
                #        "chunksize"      : ...
                #        }
                metadata["filesize"] = None
                metadata["file_mod_time"] = time.time()
                metadata["file_create_time"] = time.time()
                # chunksize is coming from zmq_events

                self.log.debug("metadata = {}".format(metadata))
            except:
                self.log.error("Unable to assemble multi-part message.",
                               exc_info=True)
                raise

    def send_data(self, targets, metadata, open_connections):

        if not targets:
            return

        # reading source file into memory
        try:
            self.log.debug("Getting data out of queue for file '{}'..."
                           .format(self.source_file))
            data = self.socket.recv_pyobj()
        except:
            self.log.error("Unable to get data out of queue for file '{}'"
                           .format(self.source_file), exc_info=True)
            raise

    #    try:
    #        chunksize = metadata["chunksize"]
    #    except:
    #        self.log.error("Unable to get chunksize", exc_info=True)

        try:
            self.log.debug("Packing multipart-message for file {}..."
                           .format(self.source_file))
            chunk_number = 0

            # assemble metadata for zmq-message
            metadata_extended = metadata.copy()
            metadata_extended["chunk_number"] = chunk_number

            payload = []
            payload.append(json.dumps(metadata_extended).encode("utf-8"))
            pickled_data = msgpack.packb(data)
            payload.append(pickled_data)
        except:
            self.log.error("Unable to pack multipart-message for file '{}'"
                           .format(self.source_file), exc_info=True)

        # send message
        try:
            self.send_to_targets(targets, open_connections, metadata_extended,
                                 payload)
            self.log.debug("Passing multipart-message for file '{}'...done."
                           .format(self.source_file))
        except:
            self.log.error("Unable to send multipart-message for file '{}'"
                           .format(self.source_file), exc_info=True)

    def finish(self, targets, metadata, open_connections):
        pass

    def stop(self):
        # Close zmq socket
        if self.socket is not None:
            self.socket.close(0)
            self.socket = None


if __name__ == '__main__':
    import tempfile
    from multiprocessing import Queue
    from logutils.queue import QueueHandler
    from __init__ import BASE_PATH

    def start_logging(log_filename):
        log_file = os.path.join(BASE_PATH, "logs", log_filename)
        log_size = 10485760
        verbose = True
        onscreen = "debug"

        log_queue = Queue(-1)

        # Get the log Configuration for the lisener
        if onscreen:
            # Get the log Configuration for the lisener
            h1, h2 = helpers.get_log_handlers(logfile=log_file,
                                              logsize=log_size,
                                              verbose=verbose,
                                              onscreen_log_level=onscreen)

            # Start queue listener using the stream handler above.
            log_queue_listener = helpers.CustomQueueListener(log_queue,
                                                             h1,
                                                             h2)
        else:
            # Get the log Configuration for the lisener
            h1 = helpers.get_log_handlers(logfile=log_file,
                                          logsize=log_size,
                                          verbose=verbose,
                                          onscreen_log_level=onscreen)

            # Start queue listener using the stream handler above
            log_queue_listener = helpers.CustomQueueListener(log_queue, h1)

        log_queue_listener.start()

        return log_queue, log_queue_listener

    def stop_logging(log_queue, log_queue_listener):
        log_queue.put_nowait(None)
        log_queue_listener.stop()

    def set_up_ipc_path():
        ipc_path = os.path.join(tempfile.gettempdir(), "hidra")

        if not os.path.exists(ipc_path):
            os.mkdir(ipc_path)
            # the permission have to changed explicitly because
            # on some platform they are ignored when called within mkdir
            os.chmod(ipc_path, 0o777)
            logging.info("Creating directory for IPC communication: {0}"
                         .format(ipc_path))

        return ipc_path

    log_queue, log_queue_listener = start_logging("xfel_fetcher.log")

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    ipc_path = set_up_ipc_path()
    current_pid = os.getpid()

    # Set up config
    context = zmq.Context()
    local_target = os.path.join(BASE_PATH, "data", "target")

    config = {
        "context": context,
        "n_connectors": 2,
        "xfel_connections": (("localhost", 4545), ("localhost", 4546)),
        "fix_subdirs": ["commissioning", "current", "local"],
        "store_data": False,
        "remove_data": False,
        "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
        "cleaner_job_con_str": None,
        "ipc_path": ipc_path,
        "main_pid": current_pid,
        "local_target": None,
        "sync_buffer_size": 20
    }

    # Set up receiver simulator
    receiving_port = "6005"
    ext_ip = "0.0.0.0"

    receiving_socket = context.socket(zmq.PULL)
    connection_str = "tcp://{}:{}".format(ext_ip, receiving_port)
    receiving_socket.bind(connection_str)
    logging.info("=== receiving_socket connected to {}"
                 .format(connection_str))

    # Set up simulation for data sending from event detector
    data_fetcher_con_str = ("ipc://{}/{}_{}"
                            .format(config["ipc_path"],
                                    config["main_pid"],
                                    "data_fetcher"))

    # configure required parameters
    targets = [['localhost:{}'.format(receiving_port), 1, "data"]]
    open_connections = dict()

    #use_eventdetector = True
    use_eventdetector = False

    if use_eventdetector:
        import sys

        EVENTDETECTOR_PATH = os.path.join(BASE_PATH, "src", "sender", "eventdetectors")

        if EVENTDETECTOR_PATH not in sys.path:
            sys.path.insert(0, EVENTDETECTOR_PATH)

        from xfel_events import EventDetector

        eventdetector = EventDetector(config, log_queue)
    else:
        data_fetcher_socket = context.socket(zmq.PUSH)
        data_fetcher_socket.bind(data_fetcher_con_str)
        logging.info("Start data_out_socket (bind): '{}'"
                         .format(data_fetcher_con_str))

        data = {"test": "header_trainid"}


    try:
        datafetcher = DataFetcher(config, log_queue, 0, context)

        if use_eventdetector:
            event_list = []

            while not event_list:
                event_list = eventdetector.get_new_event()

            logging.debug("event_list: {}".format(event_list))
            metadata = event_list[0]
        else:
            metadata = {"filename": 15202629571}

            logging.debug("Sending fake data.")
            data_fetcher_socket.send_pyobj(data)
            logging.debug("Done.")

        datafetcher.get_metadata(targets, metadata)

        datafetcher.send_data(targets, metadata, open_connections)

        datafetcher.finish(targets, metadata, open_connections)

        recv_data = receiving_socket.recv_multipart()

        if use_eventdetector:
            logging.debug("data received")
        else:
            logging.debug("data received {}".format(recv_data))

    finally:
        receiving_socket.close(0)
        if use_eventdetector:
            eventdetector.stop()
        else:
            data_fetcher_socket.close(0)

        context.destroy()
        stop_logging(log_queue, log_queue_listener)
