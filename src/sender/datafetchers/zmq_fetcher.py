from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import time

from datafetcherbase import DataFetcherBase
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id, context):

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "zmq_fetcher-{0}".format(id),
                                 context)

        if helpers.is_windows():
            required_params = ["ext_ip",
                               "data_fetcher_port"]
        else:
            required_params = ["ipc_path"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            self.config,
                                                            self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {0}"
                          .format(config_reduced))

            if helpers.is_windows():
                con_str = ("tcp://{0}:{1}"
                           .format(self.config["ext_ip"],
                                   self.config["data_fetcher_port"]))
            else:
                con_str = ("ipc://{0}/{1}"
                           .format(self.config["ipc_path"], "dataFetch"))

            # Create zmq socket
            try:
                self.socket = self.context.socket(zmq.PULL)
                self.socket.bind(con_str)
                self.log.info("Start socket (bind): '{0}'".format(con_str))
            except:
                self.log.error("Failed to start com_socket (bind): '{0}'"
                               .format(con_str), exc_info=True)
                raise
        else:
            self.log.debug("config={0}".format(self.config))
            raise Exception("Wrong configuration")

    def get_metadata(self, targets, metadata):

        # extract event metadata
        try:
            # TODO validate metadata dict
            self.source_file = metadata["filename"]
        except:
            self.log.error("Invalid fileEvent message received.",
                           exc_info=True)
            self.log.debug("metadata={0}".format(metadata))
            # skip all further instructions and continue with next iteration
            raise

        # TODO combine better with source_file... (for efficiency)
        if config["local_target"]:
            self.target_file = os.path.join(config["local_target"],
                                            self.source_file)
        else:
            self.target_file = None

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

                self.log.debug("metadata = {0}".format(metadata))
            except:
                self.log.error("Unable to assemble multi-part message.",
                               exc_info=True)
                raise

    def send_data(self, targets, metadata, open_connections):

        if not targets:
            return

        # reading source file into memory
        try:
            self.log.debug("Getting data out of queue for file '{0}'..."
                           .format(self.source_file))
            data = self.socket.recv()
        except:
            self.log.error("Unable to get data out of queue for file '{0}'"
                           .format(self.source_file), exc_info=True)
            raise

    #    try:
    #        chunksize = metadata["chunksize"]
    #    except:
    #        self.log.error("Unable to get chunksize", exc_info=True)

        try:
            self.log.debug("Packing multipart-message for file {0}..."
                           .format(self.source_file))
            chunk_number = 0

            # assemble metadata for zmq-message
            metadata_extended = metadata.copy()
            metadata_extended["chunk_number"] = chunk_number

            payload = []
            payload.append(json.dumps(metadata_extended).encode("utf-8"))
            payload.append(data)
        except:
            self.log.error("Unable to pack multipart-message for file '{0}'"
                           .format(self.source_file), exc_info=True)

        # send message
        try:
            self.send_to_targets(targets, open_connections, metadata_extended,
                                 payload)
            self.log.debug("Passing multipart-message for file '{0}'...done."
                           .format(self.source_file))
        except:
            self.log.error("Unable to send multipart-message for file '{0}'"
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
    import socket

    ### Set up logging ###
    logfile = os.path.join(BASE_PATH, "logs", "zmq_fetcher.log")
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

    ### determine socket connection strings ###
    con_ip = socket.gethostname()
    ext_ip = socket.gethostbyaddr(con_ip)[2][0]
    #ext_ip = "0.0.0.0"

    current_pid = os.getpid()

    cleaner_port = 50051
    confirmation_port = 50052

    ipc_path = os.path.join(tempfile.gettempdir(), "hidra")
    if not os.path.exists(ipc_path):
        os.mkdir(ipc_path)
        # the permission have to changed explicitly because
        # on some platform they are ignored when called within mkdir
        os.chmod(ipc_path, 0o777)
        logging.info("Creating directory for IPC communication: {0}"
                     .format(ipc_path))

    if helpers.is_windows():
        job_con_str = "tcp://{0}:{1}".format(con_ip, cleaner_port)
        job_bind_str = "tcp://{0}:{1}".format(ext_ip, cleaner_port)
    else:
        job_con_str = ("ipc://{0}/{1}_{2}".format(ipc_path,
                                                  current_pid,
                                                  "cleaner"))
        job_bind_str = job_con_str

    conf_con_str = "tcp://{0}:{1}".format(con_ip, confirmation_port)
    conf_bind_str = "tcp://{0}:{1}".format(ext_ip, confirmation_port)

    ### Set up config ###
    context = zmq.Context()
    local_target = os.path.join(BASE_PATH, "data", "target")

    config = {
        "type": "getFromZmq",
        "context": context,
        "ipc_path": ipc_path,
        "ext_ip": ext_ip,
        "remove_data": False,
        "cleaner_job_con_str": job_bind_str,
        "cleaner_conf_con_str": conf_bind_str,
        "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
        "local_target": None
    }

    ### Set up receiver simulator ###
    receiving_port = "6005"
    receiving_port2 = "6006"

    data_fetch_con_str = "ipc://{0}/{1}".format(ipc_path, "dataFetch")

    data_fw_socket = context.socket(zmq.PUSH)
    data_fw_socket.connect(data_fetch_con_str)
    logging.info("=== Start dataFwsocket (connect): '{0}'"
                 .format(data_fetch_con_str))

    receiving_socket = context.socket(zmq.PULL)
    connection_str = ("tcp://{0}:{1}"
                      .format(ext_ip, receiving_port))
    receiving_socket.bind(connection_str)
    logging.info("=== receiving_socket connected to {0}"
                 .format(connection_str))

    receiving_socket2 = context.socket(zmq.PULL)
    connection_str = ("tcp://{0}:{1}"
                      .format(ext_ip, receiving_port2))
    receiving_socket2.bind(connection_str)
    logging.info("=== receiving_socket2 connected to {0}"
                 .format(connection_str))

    ### Test file fetcher ###
    prework_source_file = os.path.join(BASE_PATH, "test_file.cbf")

    # read file to send it in data pipe
    file_descriptor = open(prework_source_file, "rb")
    file_content = file_descriptor.read()
    logging.debug("=== File read")
    file_descriptor.close()

    data_fw_socket.send(file_content)
    logging.debug("=== File send")

    metadata = {
        "source_path": os.path.join(BASE_PATH, "data", "source"),
        "relative_path": os.sep + "local" + os.sep + "raw",
        "filename": "100.cbf"
    }
    targets = [['{0}:{1}'.format(ext_ip, receiving_port), 1, [".cbf", ".tif"],
                "data"],
               ['{0}:{1}'.format(ext_ip, receiving_port2), 1, [".cbf", ".tif"],
                "data"]]

    open_connections = dict()

    logging.debug("open_connections before function call: {0}"
                  .format(open_connections))

    datafetcher = DataFetcher(config, log_queue, 0, context)

    datafetcher.get_metadata(targets, metadata)

    datafetcher.send_data(targets, metadata, open_connections)

    datafetcher.finish(targets, metadata, open_connections)

    logging.debug("open_connections after function call: {0}"
                  .format(open_connections))

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
        data_fw_socket.close(0)
        receiving_socket.close(0)
        receiving_socket2.close(0)
        context.destroy()
