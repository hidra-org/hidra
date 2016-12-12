from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import time

from send_helpers import __send_to_targets
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def setup(log, prop):

    if helpers.is_windows():
        required_params = ["context",
                           "ext_ip",
                           "data_fetcher_port"]
    else:
        required_params = ["context",
                           "ipc_path"]

    # Check format of config
    check_passed, config_reduced = helpers.check_config(required_params,
                                                        prop,
                                                        log)

    if check_passed:
        log.info("Configuration for data fetcher: {0}"
                 .format(config_reduced))

        if helpers.is_windows():
            con_str = ("tcp://{0}:{1}"
                       .format(prop["ext_ip"], prop["data_fetcher_port"]))
        else:
            con_str = ("ipc://{0}/{1}"
                       .format(prop["ipc_path"], "dataFetch"))

        # Create zmq socket
        try:
            socket = prop["context"].socket(zmq.PULL)
            socket.bind(con_str)
            log.info("Start socket (bind): '{0}'".format(con_str))
        except:
            log.error("Failed to start com_socket (bind): '{0}'"
                      .format(con_str), exc_info=True)
            raise

        # register socket
        prop["socket"] = socket

    return check_passed


def get_metadata(log, prop, targets, metadata, chunksize, local_target=None):

    # extract fileEvent metadata
    try:
        # TODO validate metadata dict
        source_file = metadata["filename"]
    except:
        log.error("Invalid fileEvent message received.", exc_info=True)
        log.debug("metadata={0}".format(metadata))
        # skip all further instructions and continue with next iteration
        raise

    # TODO combine better with source_file... (for efficiency)
    if local_target:
        target_file = os.path.join(local_target, source_file)
    else:
        target_file = None

    if targets:
        try:
            log.debug("create metadata for source file...")
            # metadata = {
            #        "filename"       : ...,
            #        "file_mod_time"    : ...,
            #        "file_create_time" : ...,
            #        "chunksize"      : ...
            #        }
            metadata["filesize"] = None
            metadata["file_mod_time"] = time.time()
            metadata["file_create_time"] = time.time()
            # chunksize is coming from zmq_detector

            log.debug("metadata = {0}".format(metadata))
        except:
            log.error("Unable to assemble multi-part message.", exc_info=True)
            raise

    return source_file, target_file, metadata


def send_data(log, targets, source_file, target_file, metadata,
              open_connections, context, prop):

    if not targets:
        return

    # reading source file into memory
    try:
        log.debug("Getting data out of queue for file '{0}'..."
                  .format(source_file))
        data = prop["socket"].recv()
    except:
        log.error("Unable to get data out of queue for file '{0}'"
                  .format(source_file), exc_info=True)
        raise

#    try:
#        chunksize = metadata[ "chunksize" ]
#    except:
#        log.error("Unable to get chunksize", exc_info=True)

    try:
        log.debug("Packing multipart-message for file {0}..."
                  .format(source_file))
        chunk_number = 0

        # assemble metadata for zmq-message
        metadata_extended = metadata.copy()
        metadata_extended["chunk_number"] = chunk_number

        payload = []
        payload.append(json.dumps(metadata_extended).encode("utf-8"))
        payload.append(data)
    except:
        log.error("Unable to pack multipart-message for file '{0}'"
                  .format(source_file), exc_info=True)

    # send message
    try:
        __send_to_targets(log, targets, source_file, target_file,
                          open_connections, metadata_extended, payload,
                          context)
        log.debug("Passing multipart-message for file '{0}'...done."
                  .format(source_file))
    except:
        log.error("Unable to send multipart-message for file '{0}'"
                  .format(source_file), exc_info=True)


def finish_datahandling(log, targets, source_file, target_file, metadata,
                        open_connections, context, prop):
    pass


def clean(prop):
    # Close zmq socket
    if prop["socket"]:
        prop["socket"].close(0)
        prop["socket"] = None


if __name__ == '__main__':
    import tempfile

    from dataFetchers import BASE_PATH

    logfile = os.path.join(BASE_PATH, "logs", "zmq_fetcher.log")
    logsize = 10485760

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True,
                                      onscreen_log_level="debug")

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    root.addHandler(h1)
    root.addHandler(h2)

    receiving_port = "6005"
    receiving_port2 = "6006"
    ext_ip = "0.0.0.0"
    data_fetch_con_str = ("ipc://{0}/{1}"
                          .format(os.path.join(tempfile.gettempdir(),
                                               "hidra"),
                                  "dataFetch"))

    context = zmq.Context.instance()

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

    prework_source_file = os.path.join(BASE_PATH, "test_file.cbf")

    # read file to send it in data pipe
    file_descriptor = open(prework_source_file, "rb")
    file_content = file_descriptor.read()
    logging.debug("=== File read")
    file_descriptor.close()

    data_fw_socket.send(file_content)
    logging.debug("=== File send")

    workload = {
        "source_path": os.path.join(BASE_PATH, "data", "source"),
        "relative_path": os.sep + "local" + os.sep + "raw",
        "filename": "100.cbf"
        }
    targets = [['localhost:{0}'.format(receiving_port), 1, [".cbf", ".tif"],
                "data"],
               ['localhost:{0}'.format(receiving_port2), 0, [".cbf", ".tif"],
                "data"]]

    chunksize = 10485760  # = 1024*1024*10 = 10 MiB
    local_target = os.path.join(BASE_PATH, "data", "target")
    open_connections = dict()

    config = {
        "type": "getFromZmq",
        "context": context,
        "data_fetch_con_str": data_fetch_con_str
        }

    logging.debug("open_connections before function call: {0}"
                  .format(open_connections))

    setup(logging, config)

    source_file, target_file, metadata = get_metadata(logging, config,
                                                      targets, workload,
                                                      chunksize,
                                                      local_target=None)
    send_data(logging, targets, source_file, target_file, metadata,
              open_connections, context, config)

    finish_datahandling(logging, targets, source_file, target_file, metadata,
                        open_connections, context, config)

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
        clean(config)
        context.destroy()
