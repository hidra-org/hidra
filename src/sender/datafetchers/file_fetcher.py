from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import shutil
import errno

from send_helpers import __send_to_targets, DataHandlingError
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def setup(log, config):

    required_params = ["fix_subdirs",
                       "store_data",
                       "remove_data"]

    # Check format of config
    check_passed, config_reduced = helpers.check_config(required_params,
                                                        config,
                                                        log)

    if check_passed:
        log.info("Configuration for data fetcher: {0}"
                 .format(config_reduced))

        config["send_timeout"] = -1  # 10
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

    # filename = "img.tiff"
    # filepath = "C:\dir"
    #
    # -->  source_file_path = 'C:\\dir\img.tiff'
    if relative_path.startswith("/"):
        source_file_path = os.path.normpath(os.path.join(source_path,
                                                         relative_path[1:]))
    else:
        source_file_path = os.path.normpath(os.path.join(source_path,
                                                         relative_path))
    source_file = os.path.join(source_file_path, filename)

    # TODO combine better with source_file... (for efficiency)
    if local_target:
        target_file_path = os.path.normpath(os.path.join(local_target,
                                                         relative_path))
        target_file = os.path.join(target_file_path, filename)
    else:
        target_file = None

    if targets:
        try:
            log.debug("get filesize for '{0}'...".format(source_file))
            filesize = os.path.getsize(source_file)
            file_mod_time = os.stat(source_file).st_mtime
            file_create_time = os.stat(source_file).st_ctime
            # For quick testing set filesize of file as chunksize
            # chunksize can be used later on to split multipart message
#            chunksize = filesize
            log.debug("filesize({0}) = {1}".format(source_file, filesize))
            log.debug("file_mod_time({0}) = {1}"
                      .format(source_file, file_mod_time))

        except:
            log.error("Unable to create metadata dictionary.")
            raise

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
            metadata["filesize"] = filesize
            metadata["file_mod_time"] = file_mod_time
            metadata["file_create_time"] = file_create_time
            metadata["chunksize"] = chunksize

            log.debug("metadata = {0}".format(metadata))
        except:
            log.error("Unable to assemble multi-part message.")
            raise

    return source_file, target_file, metadata


def send_data(log, targets, source_file, target_file, metadata,
              open_connections, context, config):

    if not targets:
        config["remove_flag"] = True
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

    # close file
    try:
        log.debug("Closing '{0}'...".format(source_file))
        file_descriptor.close()
    except:
        log.error("Unable to close target file '{0}'".format(source_file),
                  exc_info=True)
        raise

    if not sendError:
        config["remove_flag"] = True


def __datahandling(log, source_file, target_file, action_function, metadata,
                   config):
    try:
        action_function(source_file, target_file)
    except IOError as e:

        # errno.ENOENT == "No such file or directory"
        if e.errno == errno.ENOENT:
            subdir, tmp = os.path.split(metadata["relative_path"])
            target_base_path = os.path.join(
                target_file.split(subdir + os.sep)[0], subdir)

            if metadata["relative_path"] in config["fix_subdirs"]:
                log.error("Unable to copy/move file '{0}' to '{1}': "
                          "Directory {2} is not available"
                          .format(source_file, target_file,
                                  metadata["relative_path"]))
                raise
            elif (subdir in config["fix_subdirs"]
                    and not os.path.isdir(target_base_path)):
                log.error("Unable to copy/move file '{0}' to '{1}': "
                          "Directory {2} is not available"
                          .format(source_file, target_file, subdir))
                raise
            else:
                try:
                    target_path, filename = os.path.split(target_file)
                    os.makedirs(target_path)
                    log.info("New target directory created: {0}"
                             .format(target_path))
                    action_function(source_file, target_file)
                except OSError as e:
                    log.info("Target directory creation failed, was already "
                             "created in the meantime: {0}"
                             .format(target_path))
                    action_function(source_file, target_file)
                except:
                    log.error("Unable to copy/move file '{0}' to '{1}'"
                              .format(source_file, target_file), exc_info=True)
                    log.debug("target_path: {p}".format(p=target_path))
        else:
            log.error("Unable to copy/move file '{0}' to '{1}'"
                      .format(source_file, target_file), exc_info=True)
            raise
    except:
        log.error("Unable to copy/move file '{0}' to '{1}'"
                  .format(source_file, target_file), exc_info=True)
        raise


def finish_datahandling(log, targets, source_file, target_file, metadata,
                        open_connections, context, config):

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
