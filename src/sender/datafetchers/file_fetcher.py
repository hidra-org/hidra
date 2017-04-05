from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import shutil
import errno

from datafetcherbase import DataFetcherBase, DataHandlingError
from cleanerbase import CleanerBase
from hidra import generate_filepath
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id, context):

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "file_fetcher-{0}".format(id),
                                 context)

        required_params = ["fix_subdirs",
                           "store_data"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            self.config,
                                                            self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {0}"
                          .format(config_reduced))

            self.config["send_timeout"] = -1  # 10
            self.config["remove_flag"] = False

            self.is_windows = helpers.is_windows()

            if self.config["remove_data"] == "with_confirmation":
                self.finish = self.finish_with_cleaner
            else:
                self.finish = self.finish_without_cleaner

        else:
            self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    def get_metadata(self, targets, metadata):

        # Build source file
        self.source_file = generate_filepath(metadata["source_path"],
                                             metadata)

        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config["local_target"],
                                             metadata)

        if targets:
            try:
                self.log.debug("get filesize for '{0}'..."
                               .format(self.source_file))
                filesize = os.path.getsize(self.source_file)
                file_mod_time = os.stat(self.source_file).st_mtime
                file_create_time = os.stat(self.source_file).st_ctime
                self.log.debug("filesize({0}) = {1}"
                               .format(self.source_file, filesize))
                self.log.debug("file_mod_time({0}) = {1}"
                               .format(self.source_file, file_mod_time))

            except:
                self.log.error("Unable to create metadata dictionary.")
                raise

            try:
                self.log.debug("create metadata for source file...")
                # metadata = {
                #        "filename"       : ...,
                #        "source_path"     : ...,  # in unix format
                #        "relative_path"   : ...,  # in unix format
                #        "filesize"       : ...,
                #        "file_mod_time"    : ...,
                #        "file_create_time" : ...,
                #        "chunksize"      : ...
                #        }
                if self.is_windows:
                    # path convertions is save, see:
                    # http://softwareengineering.stackexchange.com/questions/245156/is-it-safe-to-convert-windows-file-paths-to-unix-file-paths-with-a-simple-replac  # noqa E501
                    metadata["source_path"] = metadata["source_path"].replace("\\", "/")
                    metadata["relative_path"] = metadata["relative_path"].replace("\\", "/")

                metadata["filesize"] = filesize
                metadata["file_mod_time"] = file_mod_time
                metadata["file_create_time"] = file_create_time
                metadata["chunksize"] = self.config["chunksize"]
                metadata["confirmation_required"] = (
                    self.config["remove_data"] == "with_confirmation")

                self.log.debug("metadata = {0}".format(metadata))
            except:
                self.log.error("Unable to assemble multi-part message.")
                raise

    def send_data(self, targets, metadata, open_connections):

        # no targets to send data to -> data can be removed
        # (after possible local storing)
        if not targets:
            self.config["remove_flag"] = True
            return

        # find the targets requesting for data
        targets_data = [i for i in targets if i[3] == "data"]

        # no targets to send data to
        if not targets_data:
            self.config["remove_flag"] = True
            return

        self.config["remove_flag"] = False
        chunksize = metadata["chunksize"]

        chunk_number = 0
        send_error = False

        # reading source file into memory
        try:
            self.log.debug("Opening '{0}'...".format(self.source_file))
            file_descriptor = open(str(self.source_file), "rb")
        except:
            self.log.error("Unable to read source file '{0}'"
                           .format(self.source_file), exc_info=True)
            raise

        self.log.debug("Passing multipart-message for file '{0}'..."
                       .format(self.source_file))
        # sending data divided into chunks
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
                chunk_payload.append(
                    json.dumps(chunk_metadata).encode("utf-8"))
                chunk_payload.append(file_content)
            except:
                self.log.error("Unable to pack multipart-message for file "
                               "'{0}'".format(self.source_file), exc_info=True)

            # send message to data targets
            try:
                self.send_to_targets(targets_data, open_connections, None,
                                     chunk_payload)
            except DataHandlingError:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})".format(self.source_file,
                                                          chunk_number),
                               exc_info=True)
                send_error = True
            except:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})".format(self.source_file,
                                                          chunk_number),
                               exc_info=True)

            chunk_number += 1

        # close file
        try:
            self.log.debug("Closing '{0}'...".format(self.source_file))
            file_descriptor.close()
        except:
            self.log.error("Unable to close target file '{0}'"
                           .format(self.source_file), exc_info=True)
            raise

        # do not remove data until a confirmation is sent back from the
        # priority target
        if self.config["remove_data"] == "with_confirmation":
            self.config["remove_flag"] = False

        # the data was successfully sent -> mark it as removable
        elif not send_error:
            self.config["remove_flag"] = True

    def _datahandling(self, action_function, metadata):
        try:
            action_function(self.source_file, self.target_file)
        except IOError as e:

            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:
                subdir, tmp = os.path.split(metadata["relative_path"])
                target_base_path = os.path.join(
                    self.target_file.split(subdir + os.sep)[0], subdir)

                if metadata["relative_path"] in self.config["fix_subdirs"]:
                    self.log.error("Unable to copy/move file '{0}' to '{1}': "
                                   "Directory {2} is not available"
                                   .format(self.source_file, self.target_file,
                                           metadata["relative_path"]))
                    raise
                elif (subdir in self.config["fix_subdirs"]
                        and not os.path.isdir(target_base_path)):
                    self.log.error("Unable to copy/move file '{0}' to '{1}': "
                                   "Directory {2} is not available"
                                   .format(self.source_file,
                                           self.target_file,
                                           subdir))
                    raise
                else:
                    try:
                        target_path, filename = os.path.split(self.target_file)
                        os.makedirs(target_path)
                        self.log.info("New target directory created: {0}"
                                      .format(target_path))
                        action_function(self.source_file, self.target_file)
                    except OSError as e:
                        self.log.info("Target directory creation failed, was "
                                      "already created in the meantime: {0}"
                                      .format(target_path))
                        action_function(self.source_file, self.target_file)
                    except:
                        self.log.error("Unable to copy/move file '{0}' to "
                                       "'{1}'".format(self.source_file,
                                                      self.target_file),
                                       exc_info=True)
                        self.log.debug("target_path: {0}".format(target_path))
            else:
                self.log.error("Unable to copy/move file '{0}' to '{1}'"
                               .format(self.source_file, self.target_file),
                               exc_info=True)
                raise
        except:
            self.log.error("Unable to copy/move file '{0}' to '{1}'"
                           .format(self.source_file, self.target_file),
                           exc_info=True)
            raise

    def finish(self, targets, metadata, open_connections):
        # is overwritten when class is instantiated depending if a cleaner
        # class is used or not
        pass

    def finish_with_cleaner(self, targets, metadata, open_connections):

        targets_metadata = [i for i in targets if i[3] == "metadata"]

        if self.config["store_data"]:

            # copy file
            # (does not preserve file owner, group or ACLs)
            try:
                self._datahandling(shutil.copy, metadata)
                self.log.info("Copying file '{0}' ...success."
                              .format(self.source_file))
            except:
                return

        elif self.config["remove_data"]:
            self.cleaner_job_socket.send_string(self.source_file)
            self.log.debug("Forwarded to cleaner {0}".format(self.source_file))

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

    def finish_without_cleaner(self, targets, metadata, open_connections):

        targets_metadata = [i for i in targets if i[3] == "metadata"]

        if (self.config["store_data"]
                and self.config["remove_data"]
                and self.config["remove_flag"]):

            # move file
            try:
                self._datahandling(shutil.move, metadata)
                self.log.info("Moving file '{0}' ...success."
                              .format(self.source_file))
            except:
                self.log.error("Could not move file {0} to {1}"
                               .format(self.source_file, self.target_file),
                               exc_info=True)
                return

        elif self.config["store_data"]:

            # copy file
            # (does not preserve file owner, group or ACLs)
            try:
                self._datahandling(shutil.copy, metadata)
                self.log.info("Copying file '{0}' ...success."
                              .format(self.source_file))
            except:
                return

        elif self.config["remove_data"] and self.config["remove_flag"]:
            # remove file
            try:
                os.remove(self.source_file)
                self.log.info("Removing file '{0}' ...success."
                              .format(self.source_file))
            except:
                self.log.error("Unable to remove file {0}"
                               .format(self.source_file), exc_info=True)

            self.config["remove_flag"] = False

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

    def stop(self):
        pass


class Cleaner(CleanerBase):
    def remove_element(self, source_file):
        # remove file
        try:
            os.remove(source_file)
            self.log.info("Removing file '{0}' ...success"
                          .format(source_file))
        except:
            self.log.error("Unable to remove file {0}".format(source_file),
                           exc_info=True)


if __name__ == '__main__':
    import time
    from shutil import copyfile
    from multiprocessing import Queue, Process
    from logutils.queue import QueueHandler
    import socket
    import tempfile

    from __init__ import BASE_PATH

    ### Set up logging ###
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

    ### determine socket connection strings ###
    con_ip = socket.gethostname()
    ext_ip = socket.gethostbyaddr(con_ip)[2][0]

    current_pid = os.getpid()

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

    ### Set up config ###
    local_target = os.path.join(BASE_PATH, "data", "target")

    config = {
        "fix_subdirs": ["commissioning", "current", "local"],
        "store_data": False,
        "remove_data": "with_confirmation",
#        "remove_data": False,
        "cleaner_job_con_str": job_bind_str,
        "cleaner_conf_con_str": conf_bind_str,
        "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
        "local_target": None
    }

    context = zmq.Context.instance()

    ### Set up cleaner ###
    use_cleaner = (config["remove_data"] == "with_confirmation")
    if use_cleaner:
        cleaner_pr = Process(target=Cleaner,
                             args=(config,
                                   log_queue,
                                   config["cleaner_job_con_str"],
                                   config["cleaner_conf_con_str"],
                                   context))
        cleaner_pr.start()

    ### Set up receiver simulator ###
    receiving_port = "6005"
    receiving_port2 = "6006"
    ext_ip = "0.0.0.0"

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

    confirmation_socket = context.socket(zmq.PUSH)
    confirmation_socket.connect(config["cleaner_conf_con_str"])
    logging.info("=== Start confirmation_socket (connect): {0}"
                 .format(config["cleaner_conf_con_str"]))


    ### Test file fetcher ###
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

    open_connections = dict()

    logging.debug("open_connections before function call: {0}"
                  .format(open_connections))

    datafetcher = DataFetcher(config, log_queue, 0, context)

    datafetcher.get_metadata(targets, metadata)

    datafetcher.send_data(targets, metadata, open_connections)

    datafetcher.finish(targets, metadata, open_connections)

    if use_cleaner:
        confirmation_socket.send(prework_target_file.encode("utf-8"))
        logging.debug("=== confirmation sent {0}".format(prework_target_file))

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
        if use_cleaner:
            time.sleep(0.5)
        else:
            time.sleep(0.1)
        receiving_socket.close(0)
        receiving_socket2.close(0)
        if use_cleaner:
            cleaner_pr.terminate()
        context.destroy()
