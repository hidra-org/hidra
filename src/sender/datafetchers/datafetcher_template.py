from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
#import errno

from datafetcherbase import DataFetcherBase, DataHandlingError
from hidra import generate_filepath

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id, context):
        """Initial setup for this module

        Checks if all required parameters are set in the configuration
        """

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "datafetcher_template-{0}".format(id),
                                 context)

        required_params = []

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

        # Build source file
        self.source_file = generate_filepath(metadata["source_path"],
                                             metadata)

        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config["local_target"],
                                             metadata)

        # Extends metadata
        if targets:
            metadata["filesize"] = 0
            metadata["file_mod_time"] = 1481734310.6207027
            metadata["file_create_time"] = 1481734310.6207028
            metadata["chunksize"] = self.config["chunksize"]

    def send_data(self, targets, metadata, open_connections, context):

        if not targets:
            return

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_data = [i for i in targets if i[2] == "data"]

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
                self.send_to_targets(targets_data, open_connections, None,
                                     chunk_payload, context)
            except DataHandlingError:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})"
                               .format(self.source_file, chunk_number),
                               exc_info=True)
            except:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})"
                               .format(self.source_file, chunk_number),
                               exc_info=True)

    def finish(self, targets, metadata, open_connections, context):

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

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

    def stop(self):
        pass


if __name__ == '__main__':
    import time
    from shutil import copyfile

    from __init__ import BASE_PATH
    import helpers

    from multiprocessing import Queue
    from logutils.queue import QueueHandler

    log_file = os.path.join(BASE_PATH, "logs", "datafetcher_template.log")
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
    targets = [['localhost:{0}'.format(receiving_port), 1, "data"],
               ['localhost:{0}'.format(receiving_port2), 1, "data"]]

    chunksize = 10485760  # = 1024*1024*10 = 10 MiB
    open_connections = dict()

    config = {
        "chunksize": chunksize,
        "local_target": None,
        "remove_data": False,
        "cleaner_job_con_str": None
    }

    logging.debug("open_connections before function call: {0}"
                  .format(open_connections))

    datafetcher = DataFetcher(config, log_queue, 0, context)

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

        if log_queue_listener:
            logging.info("Stopping log_queue")
            log_queue.put_nowait(None)
            log_queue_listener.stop()
            log_queue_listener = None
