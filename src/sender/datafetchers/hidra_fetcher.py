from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import shutil
import time

from send_helpers import send_to_targets, DataHandlingError

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def setup(log, config):
    """Initial setup for this module

    Checks if all required parameters are set in the configuration

    Args:

        log
        config (dict)

    Returns:

        check_passed (bool): if all checks were successfull or not

    """
    if helpers.is_windows():
        required_params = ["context",
                           "data_fetch_port"]
    else:
        required_params = ["context",
                           "ipc_path",
                           "main_pid"]

    # Check format of config
    check_passed, config_reduced = helpers.check_config(required_params,
                                                        config,
                                                        log)

    if check_passed:
        log.info("Configuration for data fetcher: {0}"
                 .format(config_reduced))

        # Create zmq socket to get events
        try:
            con_str = "ipc://{0}/{1}_{2}".format(config["ipc_path"],
                                                 config["main_pid"],
                                                 "out")
            config["data_fetch_socket"] = config["context"].socket(zmq.PULL)
            config["data_fetch_socket"].connect(con_str)

            log.info("Start data fetcher socket (connect): '{0}'"
                     .format(con_str))
        except:
            self.log.error("Failed to start data fetcher socket (connect): '{0}'"
                           .format(con_str), exc_info=True)
            raise
    else:
        self.log.debug("config={0}".format(config))
        raise Exception("Wrong configuration")


    return check_passed


def get_metadata(log, targets, metadata, chunksize, local_target=None):
    """Extends the given metadata and generates paths

    Reads the metadata dictionary from the event detector and extends it with
    the mandatory entries:
        - filesize (int):  the total size of the logical file (before chunking)
        - file_mod_time (float, epoch time): modification time of the logical
                                             file in epoch
        - file_create_time (float, epoch time): creation time of the logical
                                                file in epoch
        - chunksize (int): the size of the chunks the logical message was split
                           into

    Additionally it generates the absolute source path and if a local target
    is specified the absolute target path as well.

    Args:
        log
        config (dict)
        targets (list):
            A list of targets where the data or metadata should be sent to.
            It is of the form:
                [[<node_name>:<port>, <priority>,
                  <list of file suffixes>, <request_type>], ...]
            where
                <list of file suffixes>: is a python of file types which should
                                         be send to this target. e.g. [u'.cbf']
                <request_type>: u'data' or u'metadata'
        metadata (dict): Dictionary created by the event detector containing:
            - filename
            - source_path
            - relative_path
        chunksize (int)
        local_target (str): optional

    Returns:
        source_file (str): the absolute path of the source file
        target_file (str): the absolute path for the target file

    """

    # Build source file
    if metadata["relative_path"].startswith("/"):
        source_file = os.path.normpath(
                        os.path.join(metadata["source_path"],
                                     metadata["relative_path"][1:],
                                     metadata["filename"]))
    else:
        source_file = os.path.normpath(os.path.join(metadata["source_path"],
                                                    metadata["relative_path"],
                                                    metadata["filename"]))

    # Build target file
    if local_target:
        target_file_path = os.path.normpath(
            os.path.join(local_target,
                         metadata["relative_path"]))
        target_file = os.path.join(target_file_path, metadata["filename"])
    else:
        target_file = None

    # Extends metadata
    if targets:
        if "filesize" not in metadata:
            self.log.error("Received metadata do not contain 'filesize'")
        if "file_mod_time" not in metadata:
            self.log.error("Received metadata do not contain 'file_mod_time'. "
                           "Setting it to current time")
            metadata["file_mod_time"] = time.time()
        if "file_create_time" not in metadata:
            self.log.error("Received metadata do not contain 'file_create_time'. "
                           "Setting it to current time")
            metadata["file_create_time"] = time.time()
        if "chunksize" not in metadata:
            self.log.error("Received metadata do not contain 'chunksize'. "
                           "Setting it to locally configured one")
            metadata["chunksize"] = chunksize

    return source_file, target_file


def send_data(log, targets, source_file, target_file, metadata,
              open_connections, context, config):
    """Reads data into buffer and sends it to all targets

    Args:
        log
        targets (list):
            A list of targets where the data or metadata should be sent to.
            It is of the form:
                [[<node_name>:<port>, <priority>,
                  <list of file suffixes>, <request_type>], ...]
            where
                <list of file suffixes>: is a python of file types which should
                                         be send to this target. e.g. [u'.cbf']
                <request_type>: u'data' or u'metadata'
        source_file (str)
        target_file (str)
        metadata (dict): extendet metadata dictionary filled by function
                         get_metadata
        open_connections (dict)
        context: zmq context
        config (dict): modul config

    Returns:
        Nothing
    """

    if not targets:
        return

    targets_data = [i for i in targets if i[3] == "data"]

    if not targets_data:
        return

    received_data = config["data_fetch_socket"].recv_multipart()[1]
    log.debug("Received data for file {0} (chunknumber {1})"
              .format(source_file, metadata["chunk_number"]))

    log.debug("Passing multipart-message for file '{0}'..."
              .format(source_file))
    for i in range(5):

        try:
            chunk_payload = [json.dumps(metadata).encode("utf-8"), received_data]
        except:
            log.error("Unable to pack multipart-message for file '{0}'"
                      .format(source_file), exc_info=True)

        # send message to data targets
        try:
            send_to_targets(log, targets_data, source_file, target_file,
                              open_connections, None, chunk_payload, context)
        except DataHandlingError:
            log.error("Unable to send multipart-message for file '{0}' "
                      "(chunk {1})".format(source_file, chunk_number),
                      exc_info=True)
        except:
            log.error("Unable to send multipart-message for file '{0}' "
                      "(chunk {1})".format(source_file, chunk_number),
                      exc_info=True)


def finish_datahandling(log, targets, source_file, target_file, metadata,
                        open_connections, context, config):
    """

    Args:
        log
        targets (list):
            A list of targets where the data or metadata should be sent to.
            It is of the form:
                [[<node_name>:<port>, <priority>,
                  <list of file suffixes>, <request_type>], ...]
            where
                <list of file suffixes>: is a python of file types which should
                                         be send to this target. e.g. [u'.cbf']
                <request_type>: u'data' or u'metadata'
        source_file (str)
        target_file (str)
        metadata (dict)
        open_connections
        context
        config

    Returns:
        Nothing
    """

    targets_metadata = [i for i in targets if i[3] == "metadata"]

    # send message to metadata targets
    if targets_metadata:
        try:
            send_to_targets(log, targets_metadata, source_file, target_file,
                              open_connections, metadata, None, context,
                              config["send_timeout"])
            log.debug("Passing metadata multipart-message for file {0}...done."
                      .format(source_file))

        except:
            log.error("Unable to send metadata multipart-message for file "
                      "'{0}' to '{1}'".format(source_file, targets_metadata),
                      exc_info=True)


def clean(config):
    """

    Args:
        config
    """
    # Close zmq socket
    if config["data_fetch_socket"]:
        config["data_fetch_socket"].close(0)
        config["data_fetch_socket"] = None


if __name__ == '__main__':
    import time
    from shutil import copyfile
    import tempfile

    from __init__ import BASE_PATH
    import helpers

    logfile = os.path.join(BASE_PATH, "logs", "hidra_fetcher.log")
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

    config = {
        "context": context,
        "ext_ip": "127.0.0.1",
        "ipc_path": os.path.join(tempfile.gettempdir(), "hidra"),
        "main_pid": os.getpid(),
        "ext_data_port": "5559"
    }

    fw_con_str = "ipc://{0}/{1}_{2}".format(config["ipc_path"],
                                            config["main_pid"],
                                            "out")

    # create zmq socket to send events
    data_fw_socket = context.socket(zmq.PUSH)
    data_fw_socket.bind(fw_con_str)
    logging.info("Start data_fw_socket (bind): '{0}'"
                 .format(fw_con_str))


    prework_source_file = os.path.join(BASE_PATH, "test_file.cbf")
    chunksize = 10485760  # = 1024*1024*10 = 10 MiB

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

    targets = [['localhost:{0}'.format(receiving_port), 1, [".cbf"], "data"],
               ['localhost:{0}'.format(receiving_port2), 0, [".cbf"], "data"]]

    open_connections = dict()

    logging.debug("open_connections before function call: {0}"
                  .format(open_connections))

    setup(logging, config)

    # simulatate data input sent by an other HiDRA instance
    with open(prework_source_file, 'rb') as file_descriptor:
        file_content = file_descriptor.read(chunksize)
    data_fw_socket.send_multipart([json.dumps(metadata), file_content])
    logging.debug("Incoming data sent")

    source_file, target_file = get_metadata(logging, targets,
                                            metadata, chunksize,
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
        receiving_socket.close(0)
        receiving_socket2.close(0)
        data_fw_socket.close(0)
        clean(config)
        context.destroy()
