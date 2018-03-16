from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import requests
import time
import errno

from datafetcherbase import DataFetcherBase
from cleanerbase import CleanerBase
from hidra import generate_filepath
import utils

__author__ = ('Manuela Kuhn <manuela.kuhn@desy.de>',
              'Jan Garrevoet <jan.garrevoet@desy.de>')


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id, context):

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "http_fetcher-{}".format(id),
                                 context)

        required_params = ["session",
                           "store_data",
                           "remove_data",
                           "fix_subdirs"]

        # Check format of config
        check_passed, config_reduced = utils.check_config(required_params,
                                                          self.config,
                                                          self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {0}"
                          .format(config_reduced))

            self.config["session"] = requests.session()
            self.config["remove_flag"] = False

            if self.config["remove_data"] == "with_confirmation":
                self.finish = self.finish_with_cleaner
            else:
                self.finish = self.finish_without_cleaner
        else:
            # self.log.debug("config={0}".format(self.config))
            raise Exception("Wrong configuration")

    def get_metadata(self, targets, metadata):

        # no normpath used because that would transform http://...
        # into http:/...
        self.source_file = os.path.join(metadata["source_path"],
                                        metadata["relative_path"],
                                        metadata["filename"])

        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config["local_target"],
                                             metadata)

        metadata["chunksize"] = self.config["chunksize"]

        if targets:
            try:
                self.log.debug("create metadata for source file...")
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
                metadata["confirmation_required"] = (
                    self.config["remove_data"] == "with_confirmation")

                self.log.debug("metadata = {}".format(metadata))
            except:
                self.log.error("Unable to assemble multi-part message.",
                               exc_info=True)
                raise

    def send_data(self, targets, metadata, open_connections):

        response = self.config["session"].get(self.source_file)
        try:
            response.raise_for_status()
            self.log.debug("Initiating http get for file '{}' succeeded."
                           .format(self.source_file))
        except:
            self.log.error("Initiating http get for file '{}' failed."
                           .format(self.source_file), exc_info=True)
            return

        try:
            chunksize = metadata["chunksize"]
        except:
            self.log.error("Unable to get chunksize", exc_info=True)

        file_opened = False
        file_written = True
        file_closed = False
        file_send = True

        if self.config["store_data"]:
            try:
                self.log.debug("Opening '{}'...".format(self.target_file))
                file_descriptor = open(self.target_file, "wb")
                file_opened = True
            except IOError as e:
                # errno.ENOENT == "No such file or directory"
                if e.errno == errno.ENOENT:

                    # the directories current, commissioning and local should not be created
                    subdir, tmp = os.path.split(metadata["relative_path"])

                    if metadata["relative_path"] in self.config["fix_subdirs"]:
                        self.log.error("Unable to move file '{}' to '{}': "
                                       "Directory {} is not available."
                                       .format(self.source_file,
                                               self.target_file,
                                               metadata["relative_path"]),
                                       exc_info=True)

                    elif subdir in self.config["fix_subdirs"]:
                        self.log.error("Unable to move file '{}' to '{}': "
                                       "Directory {} is not available."
                                       .format(self.source_file,
                                               self.target_file,
                                               subdir),
                                       exc_info=True)
                    else:
                        try:
                            target_path, filename = (
                                os.path.split(self.target_file))
                            os.makedirs(target_path)
                            file_descriptor = open(self.target_file, "wb")
                            self.log.info("New target directory created: {}"
                                          .format(target_path))
                            file_opened = True
                        except OSError as e:
                            self.log.info("Target directory creation failed, "
                                          "was already created in the "
                                          "meantime: {}".format(target_path))
                            file_descriptor = open(self.target_file, "wb")
                            file_opened = True
                        except:
                            self.log.error("Unable to open target file '{}'."
                                           .format(self.target_file),
                                           exc_info=True)
                            self.log.debug("target_path: {}"
                                           .format(target_path))
                            raise
                else:
                    self.log.error("Unable to open target file '{}'."
                                   .format(self.target_file), exc_info=True)
            except:
                self.log.error("Unable to open target file '{}'."
                               .format(self.target_file), exc_info=True)

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_data = [i for i in targets if i[2] == "data"]
        targets_metadata = [i for i in targets if i[2] == "metadata"]
        chunk_number = 0

        self.log.debug("Getting data for file '{}'..."
                       .format(self.source_file))
        # reading source file into memory
        for data in response.iter_content(chunk_size=chunksize):
            self.log.debug("Packing multipart-message for file '{}'..."
                           .format(self.source_file))

            try:
                # assemble metadata for zmq-message
                metadata_extended = metadata.copy()
                metadata_extended["chunk_number"] = chunk_number

                payload = []
                payload.append(json.dumps(metadata_extended).encode("utf-8"))
                payload.append(data)
            except:
                self.log.error("Unable to pack multipart-message for file "
                               "'{}'".format(self.source_file),
                               exc_info=True)

            if self.config["store_data"] and file_opened:
                try:
                    file_descriptor.write(data)
                    self.log.debug("Writing data for file '{}' (chunk {})"
                                   .format(self.source_file, chunk_number))
                except:
                    self.log.error("Unable write data for file '{}'"
                                   .format(self.source_file), exc_info=True)
                    file_written = False

            # send message to data targets
            try:
                self.send_to_targets(targets_data, open_connections,
                                     metadata_extended, payload)
                self.log.debug("Passing multipart-message for file {}...done."
                               .format(self.source_file))

            except:
                self.log.error("Unable to send multipart-message for file {}"
                               .format(self.source_file), exc_info=True)
                file_send = False

            chunk_number += 1

        if self.config["store_data"] and file_opened:
            try:
                self.log.debug("Closing '{}'...".format(self.target_file))
                file_descriptor.close()
                file_closed = True
            except:
                self.log.error("Unable to close target file '{}'."
                               .format(self.target_file), exc_info=True)
                raise

            # update the creation and modification time
            metadata_extended["file_mod_time"] = (
                os.stat(self.target_file).st_mtime)
            metadata_extended["file_create_time"] = (
                os.stat(self.target_file).st_ctime)

            # send message to metadata targets
            try:
                self.send_to_targets(targets_metadata, open_connections,
                                     metadata_extended, payload)
                self.log.debug("Passing metadata multipart-message for file "
                               "'{}'...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{}'".format(self.source_file),
                               exc_info=True)

            self.config["remove_flag"] = (file_opened
                                          and file_written
                                          and file_closed)
        else:
            self.config["remove_flag"] = file_send

    def finish(self, targets, metadata, open_connections):
        # is overwritten when class is instantiated depending if a cleaner
        # class is used or not
        pass

    def finish_with_cleaner(self, targets, metadata, open_connections):

        file_id = self.generate_file_id(metadata)

        self.cleaner_job_socket.send_multipart(
            [metadata["source_path"].encode("utf-8"),
             file_id.encode("utf-8")])
        self.log.debug("Forwarded to cleaner {}".format(file_id))

    def finish_without_cleaner(self, targets, metadata, open_connections):

        if self.config["remove_data"] and self.config["remove_flag"]:
            responce = requests.delete(self.source_file)

            try:
                responce.raise_for_status()
                self.log.debug("Deleting file '{}' succeeded."
                               .format(self.source_file))
            except:
                self.log.error("Deleting file '{}' failed."
                               .format(self.source_file), exc_info=True)

    def stop(self):
        pass

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


class Cleaner(CleanerBase):
    def remove_element(self, base_path, file_id):

        # generate file path
        source_file = os.path.join(base_path, file_id)

        # remove file
        responce = requests.delete(source_file)

        try:
            responce.raise_for_status()
            self.log.debug("Deleting file '{0}' succeeded."
                           .format(source_file))
        except:
            self.log.error("Deleting file '{0}' failed."
                           .format(source_file), exc_info=True)


if __name__ == '__main__':
    import subprocess
    from multiprocessing import Queue
    from logutils.queue import QueueHandler
    from __init__ import BASE_PATH
    import socket
    import tempfile

    """ Set up logging """
    logfile = os.path.join(BASE_PATH, "logs", "http_fetcher.log")
    logsize = 10485760

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = utils.get_log_handlers(logfile, logsize, verbose=True,
                                    onscreen_log_level="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = utils.CustomQueueListener(log_queue, h1, h2)
    log_queue_listener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    """ determine socket connection strings """
    con_ip = socket.getfqdn()
    ext_ip = socket.gethostbyaddr(con_ip)[2][0]
    # ext_ip = "0.0.0.0"

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

    if utils.is_windows():
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
    config = {
        "session": None,
        "fix_subdirs": ["commissioning", "current", "local"],
        "store_data": True,
        "remove_data": False,
        "cleaner_job_con_str": job_bind_str,
        "cleaner_conf_con_str": conf_bind_str,
        "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
        "local_target": os.path.join(BASE_PATH, "data", "target")
    }

    context = zmq.Context.instance()

    """ Set up receiver simulator """
    receiving_port = "6005"
    receiving_port2 = "6006"
    dataFwPort = "50010"

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

    """ Test file fetcher """
    filename = "test01.cbf"
    prework_source_file = os.path.join(BASE_PATH, "test_file.cbf")

    # read file to send it in data pipe
    logging.debug("=== copy file to asap3-mon")
    # os.system('scp "%s" "%s:%s"' % (localfile, remotehost, remotefile) )
    subprocess.call("scp {0} root@asap3-mon:/var/www/html/data/{1}"
                    .format(prework_source_file, filename), shell=True)

    metadata = {
        "source_path": "http://asap3-mon/data",
        "relative_path": "",
        "filename": filename
    }
    targets = [['{0}:{1}'.format(ext_ip, receiving_port), 1, "data"],
               ['{0}:{1}'.format(ext_ip, receiving_port2), 1, "data"]]

    open_connections = dict()

    datafetcher = DataFetcher(config, log_queue, 0, context)

    datafetcher.get_metadata(targets, metadata)
    # source_file = "http://131.169.55.170/test_httpget/data/test_file.cbf"

    datafetcher.send_data(targets, metadata, open_connections)

    datafetcher.finish(targets, metadata, open_connections)

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

        subprocess.call('ssh root@asap3-mon rm "/var/www/html/data/{0}"'
                        .format(filename), shell=True)

        receiving_socket.close(0)
        receiving_socket2.close(0)
        context.destroy()

        if log_queue_listener:
            logging.info("Stopping log_queue")
            log_queue.put_nowait(None)
            log_queue_listener.stop()
            log_queue_listener = None
