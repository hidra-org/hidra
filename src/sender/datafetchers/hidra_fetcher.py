from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import time

from send_helpers import send_to_targets, DataHandlingError
from __init__ import BASE_PATH
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher():

    def __init__(self, config, log_queue, id):

        self.id = id
        self.config = config

        self.log = helpers.get_logger("hidra_fetcher-{0}".format(self.id),
                                      log_queue)

        self.source_file = None
        self.target_file = None

    def setup(self):
        """Initial setup for this module

        Checks if all required parameters are set in the configuration

        Returns:

            check_passed (bool): if all checks were successful or not

        """
        if helpers.is_windows():
            required_params = ["context",
                               "data_fetch_port",
                               "chunksize",
                               "local_target"]
        else:
            required_params = ["context",
                               "ipc_path",
                               "main_pid",
                               "chunksize",
                               "local_target"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            self.config,
                                                            self.log)

        context = zmq.Context()

        if check_passed:
            self.log.info("Configuration for data fetcher: {0}"
                          .format(config_reduced))

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

        return check_passed

    def get_metadata(self, targets, metadata):
        """Extends the given metadata and generates paths

        Reads the metadata dictionary from the event detector and extends it
        with the mandatory entries:
            - filesize (int):  the total size of the logical file (before
                               chunking)
            - file_mod_time (float, epoch time): modification time of the
                                                 logical file in epoch
            - file_create_time (float, epoch time): creation time of the
                                                    logical file in epoch
            - chunksize (int): the size of the chunks the logical message was
                               split into

        Additionally it generates the absolute source path and if a local
        target is specified the absolute target path as well.

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>,
                      <list of file suffixes>, <request_type>], ...]
                where
                    <list of file suffixes>: is a python of file types which
                                             should be send to this target.
                                             e.g. [u'.cbf']
                    <request_type>: u'data' or u'metadata'
            metadata (dict): Dictionary created by the event detector
                             containing:
                                - filename
                                - source_path
                                - relative_path

        Sets:
            source_file (str): the absolute path of the source file
            target_file (str): the absolute path for the target file

        """

        # Build source file
        if metadata["relative_path"].startswith("/"):
            metadata["relative_path"] = metadata["relative_path"][1:]

        self.source_file = (os.path.normpath(
            os.path.join(metadata["source_path"],
                         metadata["relative_path"],
                         metadata["filename"])))

        # Build target file
        if self.config["local_target"]:
            target_file_path = os.path.normpath(
                os.path.join(self.config["local_target"],
                             metadata["relative_path"]))
            self.target_file = os.path.join(target_file_path,
                                            metadata["filename"])
        else:
            self.target_file = None

        # Extends metadata
        if targets:
            if "filesize" not in metadata:
                self.log.error("Received metadata do not contain 'filesize'")
            if "file_mod_time" not in metadata:
                self.log.error("Received metadata do not contain "
                               "'file_mod_time'. Setting it to current time")
                metadata["file_mod_time"] = time.time()
            if "file_create_time" not in metadata:
                self.log.error("Received metadata do not contain "
                               "'file_create_time'. Setting it to current "
                               "time")
                metadata["file_create_time"] = time.time()
            if "chunksize" not in metadata:
                self.log.error("Received metadata do not contain 'chunksize'. "
                               "Setting it to locally configured one")
                metadata["chunksize"] = self.config["chunksize"]

    def send_data(self, targets, metadata, open_connections, context):
        """Reads data into buffer and sends it to all targets

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>,
                      <list of file suffixes>, <request_type>], ...]
                where
                    <list of file suffixes>: is a python of file types which
                                             should be send to this target.
                                             e.g. [u'.cbf']
                    <request_type>: u'data' or u'metadata'
            metadata (dict): extendet metadata dictionary filled by function
                             get_metadata
            open_connections (dict)
            context: zmq context

        Returns:
            Nothing
        """

        if not targets:
            return

        targets_data = [i for i in targets if i[3] == "data"]

        if not targets_data:
            return

        received_data = self.config["data_fetch_socket"].recv_multipart()[1]
        self.log.debug("Received data for file {0} (chunknumber {1})"
                       .format(self.source_file, metadata["chunk_number"]))

        self.log.debug("Passing multipart-message for file '{0}'..."
                       .format(self.source_file))
        for i in range(5):

            try:
                chunk_payload = [json.dumps(metadata).encode("utf-8"),
                                 received_data]
            except:
                self.log.error("Unable to pack multipart-message for file "
                               "'{0}'".format(self.source_file),
                               exc_info=True)

            # send message to data targets
            try:
                send_to_targets(self.log, targets_data, self.source_file,
                                self.target_file, open_connections, None,
                                chunk_payload, context)
            except DataHandlingError:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})"
                               .format(self.source_file,
                                       metadata["chunk_number"]),
                               exc_info=True)
            except:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})"
                               .format(self.source_file,
                                       metadata["chunk_number"]),
                               exc_info=True)

    def finish(self, targets, metadata, open_connections, context):
        """

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>,
                      <list of file suffixes>, <request_type>], ...]
                where
                    <list of file suffixes>: is a python of file types which
                                             should be send to this target.
                                             e.g. [u'.cbf']
                    <request_type>: u'data' or u'metadata'
            metadata (dict)
            open_connections
            context

        Returns:
            Nothing
        """

        targets_metadata = [i for i in targets if i[3] == "metadata"]

        # send message to metadata targets
        if targets_metadata:
            try:
                send_to_targets(self.log, targets_metadata, self.source_file,
                                self.target_file, open_connections, metadata,
                                None, context, self.config["send_timeout"])
                self.log.debug("Passing metadata multipart-message for file "
                               "{0}...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{0}' to '{1}'"
                               .format(self.source_file, targets_metadata),
                               exc_info=True)

    def clean(self):
        # Close zmq socket
        if self.config["data_fetch_socket"]:
            self.config["data_fetch_socket"].close(0)
            self.config["data_fetch_socket"] = None

    def __exit__(self):
        self.clean()

    def __del__(self):
        self.clean()


if __name__ == '__main__':
    import tempfile
    from multiprocessing import Queue
    from logutils.queue import QueueHandler

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
        "local_target": None
    }

    if not os.path.exists(ipc_path):
        os.mkdir(ipc_path)
        os.chmod(ipc_path, 0o777)
        logging.info("Creating directory for IPC communication: {0}"
                     .format(ipc_path))

    fw_con_str = "ipc://{0}/{1}_{2}".format(config["ipc_path"],
                                            config["main_pid"],
                                            "out")

    # create zmq socket to send events
#    data_fw_socket = context.socket(zmq.PUSH)
#    data_fw_socket.bind(fw_con_str)
#    logging.info("Start data_fw_socket (bind): '{0}'"
#                 .format(fw_con_str))

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

    datafetcher.setup()

    # simulatate data input sent by an other HiDRA instance
#    with open(prework_source_file, 'rb') as file_descriptor:
#        file_content = file_descriptor.read(chunksize)
#    data_fw_socket.send_multipart([json.dumps(metadata), file_content])
#    logging.debug("Incoming data sent")

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
#        data_fw_socket.close(0)
        context.destroy()
