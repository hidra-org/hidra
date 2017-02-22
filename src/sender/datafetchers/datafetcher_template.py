from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import shutil
#import errno

from send_helpers import __send_to_targets, DataHandlingError
import helpers

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
    check_passed = True

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

    source_file_path = os.path.normpath(os.path.join(metadata["source_path"],
                                                     metadata["relative_path"],
                                                     metadata["filename"]))

    # Build target file
    if local_target:
        target_file_path = os.path.normpath(os.path.join(local_target,
                                                         relative_path))
        target_file = os.path.join(target_file_path, filename)
    else:
        target_file = None

    # Extends metadata
    if targets:
        metadata["filesize"] = 0
        metadata["file_mod_time"] = 1481734310.6207027
        metadata["file_create_time"] = 1481734310.6207028
        metadata["chunksize"] = chunksize

    return source_file, target_file


def send_data(log, targets, source_file, target_file, metadata,
              open_connections, context, config):
    """Reads data into buffer and sends it to all targets

    Args:
        log
        targets (list)
        source_file (str)
        target_file (str)
        metadata (dict): extendet metadata dictionary filled by function get_metadata
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
        config["remove_flag"] = True
        return

    config["remove_flag"] = False
    chunksize = metadata["chunksize"]

    chunk_number = 0
    sendError = False

    # reading source file into memory
    try:
        log.debug("Opening '{0}'...".format(source_file))
        file_descriptor = open(str(source_file), "rb")
    except:
        log.error("Unable to read source file '{0}'".format(source_file),
                  exc_info=True)
        raise

    log.debug("Passing multipart-message for file '{0}'..."
              .format(source_file))
    while True:

        # read next chunk from file
        file_content = file_descriptor.read(chunksize)

        # detect if end of file has been reached
        if not file_content:
            break

        try:
            # assemble metadata for zmq-message
            chunk_metadata = metadata.copy()
            chunk_metadata["chunk_number"] = chunk_number

            chunk_payload = []
            chunk_payload.append(json.dumps(chunk_metadata).encode("utf-8"))
            chunk_payload.append(file_content)
        except:
            log.error("Unable to pack multipart-message for file '{0}'"
                      .format(source_file), exc_info=True)

        # send message to data targets
        try:
            __send_to_targets(log, targets_data, source_file, target_file,
                              open_connections, None, chunk_payload, context)
        except DataHandlingError:
            log.error("Unable to send multipart-message for file '{0}' "
                      "(chunk {1})".format(source_file, chunk_number),
                      exc_info=True)
            sendError = True
        except:
            log.error("Unable to send multipart-message for file '{0}' "
                      "(chunk {1})".format(source_file, chunk_number),
                      exc_info=True)

        chunk_number += 1


def finish_datahandling(log, targets, source_file, target_file, metadata,
                        open_connections, context, config):
    """

    Args:
        log
        targets (list)
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

    if (config["store_data"]
            and config["remove_data"]
            and config["remove_flag"]):

        # move file
        try:
            __datahandling(log, source_file, target_file, shutil.move,
                           metadata, config)
            log.info("Moving file '{0}' ...success.".format(source_file))
        except:
            log.error("Could not move file {0} to {1}"
                      .format(source_file, target_file), exc_info=True)
            return

    elif config["store_data"]:

        # copy file
        # (does not preserve file owner, group or ACLs)
        try:
            __datahandling(log, source_file, target_file, shutil.copy,
                           metadata, config)
            log.info("Copying file '{0}' ...success.".format(source_file))
        except:
            return

    elif config["remove_data"] and config["remove_flag"]:
        # remove file
        try:
            os.remove(source_file)
            log.info("Removing file '{0}' ...success.".format(source_file))
        except:
            log.error("Unable to remove file {0}".format(source_file),
                      exc_info=True)

        config["remove_flag"] = False

    # send message to metadata targets
    if targets_metadata:
        try:
            __send_to_targets(log, targets_metadata, source_file, target_file,
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
    pass


if __name__ == '__main__':
    import time
    from shutil import copyfile

    from datafetchers import BASE_PATH

    logfile = os.path.join(BASE_PATH, "logs", "file_fetcher.log")
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

    prework_source_file = os.path.join(BASE_PATH, "test_file.cbf")
    prework_target_file = os.path.join(
        BASE_PATH, "data", "source", "local", "100.cbf")

    copyfile(prework_source_file, prework_target_file)
    time.sleep(0.5)

    workload = {
        "source_path": os.path.join(BASE_PATH, "data", "source"),
        "relative_path": os.sep + "local",
        "filename": "100.cbf"
    }
    targets = [['localhost:{0}'.format(receiving_port), 1, [".cbf"], "data"],
               ['localhost:{0}'.format(receiving_port2), 0, [".cbf"], "data"]]

    chunksize = 10485760  # = 1024*1024*10 = 10 MiB
    local_target = os.path.join(BASE_PATH, "data", "target")
    open_connections = dict()

    config = {
        "fix_subdirs": ["commissioning", "current", "local"],
        "store_data": False,
        "remove_data": False
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
        receiving_socket.close(0)
        receiving_socket2.close(0)
        clean(config)
        context.destroy()
