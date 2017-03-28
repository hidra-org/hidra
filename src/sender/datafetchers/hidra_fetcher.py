from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import time

from datafetcherbase import DataFetcherBase, DataHandlingError
from hidra import Transfer, generate_filepath, store_data_chunk
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id):
        """Initial setup

        Checks if all required parameters are set in the configuration
        """

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "hidra_fetcher-{0}".format(id))

        self.f_descriptors = dict()

        if helpers.is_windows():
            required_params = ["context",
                               "data_fetch_port",
                               "store_data"]
        else:
            required_params = ["context",
                               "ipc_path",
                               "main_pid",
                               "store_data"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            self.config,
                                                            self.log)

        context = zmq.Context()

        if check_passed:
            self.log.info("Configuration for data fetcher: {0}"
                          .format(config_reduced))

            self.metadata_r = None
            self.data_r = None

            con_str = "ipc://{0}/{1}_{2}".format(self.config["ipc_path"],
                                                 self.config["main_pid"],
                                                 "out")

            # Create zmq socket to get events
            try:
                self.config["data_fetch_socket"] = context.socket(zmq.PULL)
#                self.config["data_fetch_socket"] = (
#                    self.config["context"].socket(zmq.PULL))
                self.config["data_fetch_socket"].connect(con_str)

                self.log.info("Start data fetcher socket (connect): '{0}'"
                              .format(con_str))
            except:
                self.log.error("Failed to start data fetcher socket (connect):"
                               " '{0}'".format(con_str), exc_info=True)
                raise
        else:
            self.log.debug("config={0}".format(self.config))
            raise Exception("Wrong configuration")

    def get_metadata(self, targets, metadata):

        # Get new data
        self.metadata_r, self.data_r = (
            self.config["data_fetch_socket"].recv_multipart())

        # the metadata were received as string and have to be converted into
        # a dictionary
        self.metadata_r = json.loads(self.metadata_r.decode("utf-8"))

        if ( metadata["relative_path"] != self.metadata_r["relative_path"]
                or metadata["source_path"] != self.metadata_r["source_path"]
                or metadata["filename"] != self.metadata_r["filename"]):
            self.log.error("Received metadata do not match data")

        # Use received data to prevent missmatch of metadata and data
        # TODO handle case if file type requesed by target does not match

        # Build source file
        self.source_file = generate_filepath(self.metadata_r["source_path"],
                                             self.metadata_r)

        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config["local_target"],
                                             self.metadata_r)

        # Extends metadata
        if targets:
            if "filesize" not in self.metadata_r:
                self.log.error("Received metadata do not contain 'filesize'")
            if "file_mod_time" not in self.metadata_r:
                self.log.error("Received metadata do not contain "
                               "'file_mod_time'. Setting it to current time")
                self.metadata_r["file_mod_time"] = time.time()
            if "file_create_time" not in self.metadata_r:
                self.log.error("Received metadata do not contain "
                               "'file_create_time'. Setting it to current "
                               "time")
                self.metadata_r["file_create_time"] = time.time()
            if "chunksize" not in self.metadata_r:
                self.log.error("Received metadata do not contain 'chunksize'. "
                               "Setting it to locally configured one")
                self.metadata_r["chunksize"] = self.config["chunksize"]

    def send_data(self, targets, metadata, open_connections, context):

        if not targets:
            return

        targets_data = [i for i in targets if i[3] == "data"]

        if not targets_data:
            return

        self.log.debug("Received data for file {0} (chunknumber {1})"
                       .format(self.source_file,
                               self.metadata_r["chunk_number"]))

        self.log.debug("Passing multipart-message for file '{0}'..."
                       .format(self.source_file))
        for i in range(5):

            try:
                chunk_payload = [json.dumps(self.metadata_r).encode("utf-8"),
                                 self.data_r]
            except:
                self.log.error("Unable to pack multipart-message for file "
                               "'{0}'".format(self.source_file),
                               exc_info=True)

            # send message to data targets
            try:
                self.send_to_targets(targets_data, open_connections, None,
                                     chunk_payload, context)
            except DataHandlingError:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})"
                               .format(self.source_file,
                                       self.metadata_r["chunk_number"]),
                               exc_info=True)
            except:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})"
                               .format(self.source_file,
                                       self.metadata_r["chunk_number"]),
                               exc_info=True)

    def finish(self, targets, metadata, open_connections, context):

        targets_metadata = [i for i in targets if i[3] == "metadata"]

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(targets_metadata, open_connections,
                                     metadata, None, context,
                                     self.config["send_timeout"])
                self.log.debug("Passing metadata multipart-message for file "
                               "{0}...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{0}' to '{1}'"
                               .format(self.source_file, targets_metadata),
                               exc_info=True)

        # store data
        if self.config["store_data"]:
            # TODO: save message to file using a thread (avoids blocking)
            store_data_chunk(self.f_descriptors, self.target_file,
                             self.data_r, self.config["local_target"],
                             self.metadata_r, self.log)

    def stop(self):

        # Close open file handler to prevent file corruption
        for target_file in list(self.f_descriptors.keys()):
            self.f_descriptors[target_file].close()
            del self.f_descriptors[target_file]

        # Close zmq socket
        if self.config["data_fetch_socket"]:
            self.config["data_fetch_socket"].close(0)
            self.config["data_fetch_socket"] = None


if __name__ == '__main__':
    import tempfile
    from multiprocessing import Queue
    from logutils.queue import QueueHandler
    from __init__ import BASE_PATH

    logfile = os.path.join(BASE_PATH, "logs", "hidra_fetcher.log")
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

    receiving_port = "6005"
    receiving_port2 = "6006"
    ext_ip = "0.0.0.0"

    context = zmq.Context()

    receiving_socket = context.socket(zmq.PULL)
    connection_str = "tcp://{0}:{1}".format(ext_ip, receiving_port)
    receiving_socket.bind(connection_str)
    logging.info("=== receiving_socket connected to {0}"
                 .format(connection_str))

    receiving_socket2 = context.socket(zmq.PULL)
    connection_str = "tcp://{0}:{1}".format(ext_ip, receiving_port2)
    receiving_socket2.bind(connection_str)
    logging.info("=== receiving_socket2 connected to {0}"
                 .format(connection_str))

#    current_pid = os.getpid()
    current_pid = 12345
    chunksize = 10485760  # = 1024*1024*10 = 10 MiB
    ipc_path = os.path.join(tempfile.gettempdir(), "hidra")

    config = {
        "context": context,
        "ext_ip": "131.169.185.121",
        "ipc_path": ipc_path,
        "main_pid": current_pid,
        "ext_data_port": "50100",
        "chunksize": chunksize,
        "local_target":  os.path.join(BASE_PATH, "data", "zmq_target"),
        "store_data": True
    }

    if not os.path.exists(ipc_path):
        os.mkdir(ipc_path)
        os.chmod(ipc_path, 0o777)
        logging.info("Creating directory for IPC communication: {0}"
                     .format(ipc_path))

    fw_con_str = "ipc://{0}/{1}_{2}".format(config["ipc_path"],
                                            config["main_pid"],
                                            "out")

    data_input = True

    if data_input:
        # create zmq socket to send events
        data_fw_socket = context.socket(zmq.PUSH)
        data_fw_socket.bind(fw_con_str)
        logging.info("Start data_fw_socket (bind): '{0}'"
                     .format(fw_con_str))

    prework_source_file = os.path.join(BASE_PATH, "test_file.cbf")

    metadata = {
        "source_path": os.path.join(BASE_PATH, "data", "source"),
        "relative_path": os.sep + "local",
        "filename": "100.cbf",
        "filesize": os.stat(prework_source_file).st_size,
        "file_mod_time": time.time(),
        "file_create_time": time.time(),
        "chunksize": chunksize,
        "chunk_number": 0,
    }

#    targets = [['localhost:{0}'.format(receiving_port), 1, [".cbf"], "data"]]
    targets = [['localhost:{0}'.format(receiving_port), 1, [".cbf"], "data"],
               ['localhost:{0}'.format(receiving_port2), 0, [".cbf"], "data"]]

    open_connections = dict()

    logging.debug("open_connections before function call: {0}"
                  .format(open_connections))

    datafetcher = DataFetcher(config, log_queue, 0)

    if data_input:
        # simulatate data input sent by an other HiDRA instance
        with open(prework_source_file, 'rb') as file_descriptor:
            file_content = file_descriptor.read(chunksize)
        data_fw_socket.send_multipart([json.dumps(metadata), file_content])
        logging.debug("Incoming data sent")

    datafetcher.get_metadata(targets, metadata)

    datafetcher.send_data(targets, metadata, open_connections, context)

    datafetcher.finish(targets, metadata, open_connections, context)

    logging.debug("open_connections after function call: {0}"
                  .format(open_connections))

    try:
        pass
        recv_message = receiving_socket.recv_multipart()
        logging.info("=== received: {0}"
                     .format(json.loads(recv_message[0].decode("utf-8"))))
        recv_message = receiving_socket2.recv_multipart()
        logging.info("=== received 2: {0}"
                     .format(json.loads(recv_message[0].decode("utf-8"))))
    except KeyboardInterrupt:
        pass
    finally:
        receiving_socket.close(0)
        receiving_socket2.close(0)
        if data_input:
            data_fw_socket.close(0)
        context.destroy()
