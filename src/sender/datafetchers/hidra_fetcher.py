from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import time

from datafetcherbase import DataFetcherBase, DataHandlingError
from hidra import generate_filepath, Transfer
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id, context):
        """Initial setup

        Checks if all required parameters are set in the configuration
        """

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "hidra_fetcher-{0}".format(id),
                                 context)

        self.f_descriptors = dict()
        self.transfer = None

        required_params = ["context",
                           "store_data",
                           "status_check_resp_port",
                           "confirmation_resp_port"]

        if helpers.is_windows():
            required_params += ["data_fetch_port"]
        else:
            required_params += ["ipc_path",
                                "main_pid"]

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

            self.transfer = Transfer("STREAM", use_log=log_queue)

            self.transfer.start([self.config["ipc_path"],
                                 "{0}_{1}".format(self.config["main_pid"],
                                                  "out")],
                                protocol="ipc", data_con_style="connect")

            # enable status check requests from any sender
            self.transfer.setopt("status_check",
                                 [self.config["ext_ip"],
                                  self.config["status_check_resp_port"]])

            # enable confirmation reply if this is requested in a received data
            # packet
            self.transfer.setopt("confirmation",
                                 [self.config["ext_ip"],
                                  self.config["confirmation_resp_port"]])

        else:
            # self.log.debug("config={0}".format(self.config))
            raise Exception("Wrong configuration")

    def get_metadata(self, targets, metadata):

        # Get new data
        self.metadata_r, self.data_r = self.transfer.get()

        if (metadata["relative_path"] != self.metadata_r["relative_path"]
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

    def send_data(self, targets, metadata, open_connections):

        if not targets:
            return

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_data = [i for i in targets if i[2] == "data"]

        if not targets_data:
            return

        self.log.debug("Received data for file {0} (chunknumber {1})"
                       .format(self.source_file,
                               self.metadata_r["chunk_number"]))

        self.log.debug("Passing multipart-message for file '{0}'..."
                       .format(self.source_file))

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
                                 chunk_payload)
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

    def finish(self, targets, metadata, open_connections):

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(targets_metadata, open_connections,
                                     metadata, None,
                                     self.config["send_timeout"])
                self.log.debug("Passing metadata multipart-message for file "
                               "{0}...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{0}' to '{1}'"
                               .format(self.source_file, targets_metadata),
                               exc_info=True)

        if self.config["store_data"]:
            # store data
            try:
                # TODO: save message to file using a thread (avoids blocking)
                self.transfer.store_data_chunk(self.f_descriptors,
                                               self.target_file,
                                               self.data_r,
                                               self.config["local_target"],
                                               self.metadata_r)
            except:
                self.log.error("Storing multipart message for file '{0}' "
                               "failed".format(self.source_file),
                               exc_info=True)

    def stop(self):

        # Close open file handler to prevent file corruption
        for target_file in list(self.f_descriptors.keys()):
            self.f_descriptors[target_file].close()
            del self.f_descriptors[target_file]

        # close zmq sockets
        if self.transfer is not None:
            self.transfer.stop()


if __name__ == '__main__':
    import tempfile
    from multiprocessing import Queue
    from logutils.queue import QueueHandler
    from __init__ import BASE_PATH
    import socket

    """ Set up logging """
    log_file = os.path.join(BASE_PATH, "logs", "hidra_fetcher.log")
    log_size = 10485760

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(log_file,
                                      log_size,
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

    """ determine socket connection strings """
    con_ip = socket.gethostname()
    ext_ip = socket.gethostbyaddr(con_ip)[2][0]
    # ext_ip = "0.0.0.0"

    # current_pid = os.getpid()
    current_pid = 12345

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

    """ Set up config """
    context = zmq.Context()

    config = {
        "context": context,
        "ext_ip": ext_ip,
        "ipc_path": ipc_path,
        "main_pid": current_pid,
        "ext_data_port": "50100",
        "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
        "local_target": os.path.join(BASE_PATH, "data", "zmq_target"),
        "store_data": True,
        "remove_data": False,
        "cleaner_job_con_str": None,
        "status_check_resp_port": "50011",
        "confirmation_resp_port": "50012"
    }

    """ Set up receiver simulator """
    receiving_port = "6005"
    receiving_port2 = "6006"

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

    """ Set up data forwarding simulator """
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

    """ Test file fetcher """
    prework_source_file = os.path.join(BASE_PATH, "test_file.cbf")

    metadata = {
        "source_path": os.path.join(BASE_PATH, "data", "source"),
        "relative_path": os.sep + "local",
        "filename": "100.cbf",
        "filesize": os.stat(prework_source_file).st_size,
        "file_mod_time": time.time(),
        "file_create_time": time.time(),
        "chunksize": config["chunksize"],
        "chunk_number": 0,
    }

    targets = [
        ['{0}:{1}'.format(ext_ip, receiving_port), 1, "data"],
        ['{0}:{1}'.format(ext_ip, receiving_port2), 1, "data"]
    ]

    open_connections = dict()

    logging.debug("open_connections before function call: {0}"
                  .format(open_connections))

    datafetcher = DataFetcher(config, log_queue, 0, context)

    if data_input:
        # simulatate data input sent by an other HiDRA instance
        with open(prework_source_file, 'rb') as file_descriptor:
            file_content = file_descriptor.read(config["chunksize"])
        data_fw_socket.send_multipart([json.dumps(metadata), file_content])
        logging.debug("Incoming data sent")

    datafetcher.get_metadata(targets, metadata)

    datafetcher.send_data(targets, metadata, open_connections)

    datafetcher.finish(targets, metadata, open_connections)

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

        if log_queue_listener:
            logging.info("Stopping log_queue")
            log_queue.put_nowait(None)
            log_queue_listener.stop()
            log_queue_listener = None
