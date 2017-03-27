from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
#import errno

from send_helpers import send_to_targets, DataHandlingError

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher():

    def __init__(self, config, log_queue, id):
        """Initial setup for this module

        Checks if all required parameters are set in the configuration
        """

        self.id = id
        self.config = config

        self.log = helpers.get_logger("eventdetector_template-{0}"
                                      .format(self.id), log_queue)

        self.source_file = None
        self.target_file = None

        required_params = ["chunksize",
                           "local_target"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            self.config,
                                                            self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {0}"
                          .format(config_reduced))
        else:
            self.log.debug("config={0}".format(self.config))
            raise Exception("Wrong configuration")

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
        if metadata["relative_path"].startswith("/"):
            self.source_file = os.path.normpath(
                os.path.join(metadata["source_path"],
                             metadata["relative_path"][1:],
                             metadata["filename"]))
        else:
            self.source_file = os.path.normpath(
                os.path.join(metadata["source_path"],
                             metadata["relative_path"],
                             metadata["filename"])

        # Build target file
        if self.config["local_target"] is not None:
            self.target_file = os.path.join(
                os.path.normpath(
                    os.path.join(local_target,
                                 metadata["relative_path"])),
                metadata["filename"])
        else:
            self.target_file = None

        # Extends metadata
        if targets:
            metadata["filesize"] = 0
            metadata["file_mod_time"] = 1481734310.6207027
            metadata["file_create_time"] = 1481734310.6207028
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

        self.log.debug("Passing multipart-message for file '{0}'..."
                       .format(self.source_file))
        for i in range(5):

            chunk_number = i
            file_content = b"test_data_{0}".format(chunk_number)

            try:
                # assemble metadata for zmq-message
                chunk_metadata = metadata.copy()
                chunk_metadata["chunk_number"] = chunk_number

                chunk_payload = []
                chunk_payload.append(json.dumps(chunk_metadata)
                                     .encode("utf-8"))
                chunk_payload.append(file_content)
            except:
                self.log.error("Unable to pack multipart-message for file "
                               "'{0}'".format(self.source_file), exc_info=True)

            # send message to data targets
            try:
                send_to_targets(self.log, targets_data, self.source_file,
                                self.target_file, open_connections, None,
                                chunk_payload, context)
            except DataHandlingError:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})".format(self.source_file,
                               chunk_number), exc_info=True)
            except:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})".format(self.source_file,
                               chunk_number), exc_info=True)

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

    def finish_datahandling(log, targets, source_file, target_file, metadata,
                            open_connections, context, config):

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
                               "file '{0}' to '{1}'".format(self.source_file,
                               targets_metadata), exc_info=True)

    def stop(self):
        pass

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    import time
    from shutil import copyfile

    from __init__ import BASE_PATH
    import helpers

    from multiprocessing import Queue
    from logutils.queue import QueueHandler


    logfile = os.path.join(BASE_PATH, "logs", "file_fetcher.log")
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

    context = zmq.Context.instance()

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

    prework_source_file = os.path.join(BASE_PATH, "test_file.cbf")
    prework_target_file = os.path.join(
        BASE_PATH, "data", "source", "local", "100.cbf")

    copyfile(prework_source_file, prework_target_file)
    time.sleep(0.5)

    metadata = {
        "source_path": os.path.join(BASE_PATH, "data", "source"),
        "relative_path": os.sep + "local",
        "filename": "100.cbf"
    }
    targets = [['localhost:{0}'.format(receiving_port), 1, [".cbf"], "data"],
               ['localhost:{0}'.format(receiving_port2), 0, [".cbf"], "data"]]

    chunksize = 10485760  # = 1024*1024*10 = 10 MiB
    open_connections = dict()

    config = {
        "chunksize": chunksize,
        "local_target": None
    }

    logging.debug("open_connections before function call: {0}"
                  .format(open_connections))

    datafetcher = DataFetcher(config, log_queue, 0)

    datafetcher.setup()

    datafetcher.get_metadata(targets, metadata)

    datafetcher.send_data(targets, metadata, open_connections, context)

    datafetcher.finish(targets, metadata, open_connections, context)

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
        receiving_socket.close(0)
        receiving_socket2.close(0)
        context.destroy()
