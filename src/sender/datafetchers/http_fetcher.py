from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import requests
import time
import errno

from send_helpers import send_to_targets
import helpers

__author__ = ('Manuela Kuhn <manuela.kuhn@desy.de>',
              'Jan Garrevoet <jan,garrevoet@desy.de>')


def setup(log, config):

    required_params = ["session",
                       "store_data",
                       "remove_data",
                       "fix_subdirs"]

    # Check format of config
    check_passed, config_reduced = helpers.check_config(required_params,
                                                        config,
                                                        log)

    if check_passed:
        log.info("Configuration for data fetcher: {0}"
                 .format(config_reduced))

        config["session"] = requests.session()
        config["remove_flag"] = False

    return check_passed


def get_metadata(log, config, targets, metadata, chunksize, local_target=None):

    # extract fileEvent metadata
    try:
        # TODO validate metadata dict
        filename = metadata["filename"]
        source_path = metadata["source_path"]
        relative_path = metadata["relative_path"]
    except:
        log.error("Invalid fileEvent message received.", exc_info=True)
        log.debug("metadata={0}".format(metadata))
        # skip all further instructions and continue with next iteration
        raise

    # no normpath used because that would transform http://... into http:/...
    source_file_path = os.path.join(source_path, relative_path)
    source_file = os.path.join(source_file_path, filename)

    # TODO combine better with source_file... (for efficiency)
    if local_target:
        target_file_path = os.path.normpath(os.path.join(local_target,
                                                         relative_path))
        target_file = os.path.join(target_file_path, filename)
    else:
        target_file = None

    metadata["chunksize"] = chunksize

    if targets:
        try:
            log.debug("create metadata for source file...")
            # metadata = {
            #        "filename"       : ...,
            #        "source_path"     : ...,
            #        "relative_path"   : ...,
            #        "filesize"       : ...,
            #        "file_mod_time"    : ...,
            #        "file_create_time" : ...,
            #        "chunksize"      : ...
            #        }
            metadata["file_mod_time"] = time.time()
            metadata["file_create_time"] = time.time()

            log.debug("metadata = {0}".format(metadata))
        except:
            log.error("Unable to assemble multi-part message.", exc_info=True)
            raise

    return source_file, target_file, metadata


def send_data(log, targets, source_file, target_file, metadata,
              open_connections, context, config):

    response = config["session"].get(source_file)
    try:
        response.raise_for_status()
        log.debug("Initiating http get for file '{0}' succeeded."
                  .format(source_file))
    except:
        log.error("Initiating http get for file '{0}' failed."
                  .format(source_file), exc_info=True)
        return

    try:
        chunksize = metadata["chunksize"]
    except:
        log.error("Unable to get chunksize", exc_info=True)

    file_opened = False
    file_written = True
    file_closed = False
    file_send = True

    if config["store_data"]:
        try:
            log.debug("Opening '{0}'...".format(target_file))
            file_descriptor = open(target_file, "wb")
            file_opened = True
        except IOError as e:
            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:

                subdir, tmp = os.path.split(metadata["relative_path"])

                if metadata["relative_path"] in config["fix_subdirs"]:
                    log.error("Unable to move file '{0}' to '{1}': "
                              "Directory {2} is not available."
                              .format(source_file, target_file,
                                      metadata["relative_path"]),
                              exc_info=True)

                elif subdir in config["fix_subdirs"]:
                    log.error("Unable to move file '{0}' to '{1}': "
                              "Directory {2} is not available."
                              .format(source_file, target_file, subdir),
                              exc_info=True)
                else:
                    try:
                        target_path, filename = os.path.split(target_file)
                        os.makedirs(target_path)
                        file_descriptor = open(target_file, "wb")
                        log.info("New target directory created: {0}"
                                 .format(target_path))
                        file_opened = True
                    except OSError as e:
                        log.info("Target directory creation failed, was "
                                 "already created in the meantime: {0}"
                                 .format(target_path))
                        file_descriptor = open(target_file, "wb")
                        file_opened = True
                    except:
                        log.error("Unable to open target file '{0}'."
                                  .format(target_file), exc_info=True)
                        log.debug("target_path: {0}".format(target_path))
                        raise
            else:
                log.error("Unable to open target file '{0}'."
                          .format(target_file), exc_info=True)
        except:
            log.error("Unable to open target file '{0}'."
                      .format(target_file), exc_info=True)

    targets_data = [i for i in targets if i[3] == "data"]
    targets_metadata = [i for i in targets if i[3] == "metadata"]
    chunk_number = 0

    log.debug("Getting data for file '{0}'...".format(source_file))
    # reading source file into memory
    for data in response.iter_content(chunk_size=chunksize):
        log.debug("Packing multipart-message for file '{0}'..."
                  .format(source_file))

        try:
            # assemble metadata for zmq-message
            metadata_extended = metadata.copy()
            metadata_extended["chunk_number"] = chunk_number

            payload = []
            payload.append(json.dumps(metadata_extended).encode("utf-8"))
            payload.append(data)
        except:
            log.error("Unable to pack multipart-message for file '{0}'"
                      .format(source_file), exc_info=True)

        if config["store_data"]:
            try:
                file_descriptor.write(data)
            except:
                log.error("Unable write data for file '{0}'"
                          .format(source_file), exc_info=True)
                file_written = False

        # send message to data targets
        try:
            send_to_targets(log, targets_data, source_file, target_file,
                            open_connections, metadata_extended, payload,
                            context)
            log.debug("Passing multipart-message for file {0}...done."
                      .format(source_file))

        except:
            log.error("Unable to send multipart-message for file {0}"
                      .format(source_file), exc_info=True)
            file_send = False

        chunk_number += 1

    if config["store_data"]:
        try:
            log.debug("Closing '{0}'...".format(target_file))
            file_descriptor.close()
            file_closed = True
        except:
            log.error("Unable to close target file '{0}'.".format(target_file),
                      exc_info=True)
            raise

        # update the creation and modification time
        metadata_extended["file_mod_time"] = os.stat(target_file).st_mtime
        metadata_extended["file_create_time"] = os.stat(target_file).st_ctime

        # send message to metadata targets
        try:
            send_to_targets(log, targets_metadata, source_file, target_file,
                            open_connections, metadata_extended, payload,
                            context)
            log.debug("Passing metadata multipart-message for file '{0}'"
                      "...done.".format(source_file))

        except:
            log.error("Unable to send metadata multipart-message for "
                      "file '{0}'".format(source_file), exc_info=True)

        config["remove_flag"] = file_opened and file_written and file_closed
    else:
        config["remove_flag"] = file_send


def finish_datahandling(log, targets, source_file, target_file, metadata,
                        open_connections, context, config):

    if config["remove_data"] and config["remove_flag"]:
        responce = requests.delete(source_file)

        try:
            responce.raise_for_status()
            log.debug("Deleting file '{0}' succeeded.".format(source_file))
        except:
            log.error("Deleting file '{0}' failed.".format(source_file),
                      exc_info=True)


def clean(config):
    pass


if __name__ == '__main__':
    import subprocess

    from datafetchers import BASE_PATH

    logfile = os.path.join(BASE_PATH, "logs", "http_fetcher.log")
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
    dataFwPort = "50010"

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
    local_target = os.path.join(BASE_PATH, "data", "target")

    # read file to send it in data pipe
    logging.debug("=== copy file to lsdma-lab04")
#    os.system('scp "%s" "%s:%s"' % (localfile, remotehost, remotefile) )
    subprocess.call("scp {0} root@lsdma-lab04:/var/www/html/test_httpget/data"
                    .format(prework_source_file), shell=True)

#    workload = {
#            "source_path"  : "http://192.168.138.37/data",
#            "relative_path": "",
#            "filename"    : "35_data_000170.h5"
#            }
    workload = {
        "source_path": "http://131.169.55.170/test_httpget/data",
        "relative_path": "",
        "filename": "test_file.cbf"
    }
    targets = [['localhost:{0}'.format(receiving_port), 1, [".cbf", ".tif"],
                "data"],
               ['localhost:{0}'.format(receiving_port2), 1, [".cbf", ".tif"],
                "data"]]

    chunksize = 10485760  # = 1024*1024*10 = 10 MiB
    local_target = os.path.join(BASE_PATH, "data", "target")
    open_connections = dict()

    config = {
        "session": None,
        "fix_subdirs": ["commissioning", "current", "local"],
        "store_data": True,
        "remove_data": False
    }

    setup(logging, config)

    source_file, target_file, metadata = get_metadata(logging, config,
                                                      targets, workload,
                                                      chunksize,
                                                      local_target)
#    source_file = "http://131.169.55.170/test_httpget/data/test_file.cbf"

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
        logging.info("=== received 2: {0}\ndata-part: {0}"
                     .format(json.loads(recv_message[0].decode("utf-8")),
                             len(recv_message[1])))
    except KeyboardInterrupt:
        pass
    finally:
        receiving_socket.close(0)
        receiving_socket2.close(0)
        clean(config)
        context.destroy()
