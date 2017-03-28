from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import requests
import time
import errno

from datafetcherbase import DataFetcherBase, DataHandlingError
from hidra import generate_filepath
import helpers

__author__ = ('Manuela Kuhn <manuela.kuhn@desy.de>',
              'Jan Garrevoet <jan.garrevoet@desy.de>')


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id):

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "http_fetcher-{0}".format(id))

        required_params = ["session",
                           "store_data",
                           "remove_data",
                           "fix_subdirs"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            self.config,
                                                            self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {0}"
                          .format(config_reduced))

            self.config["session"] = requests.session()
            self.config["remove_flag"] = False

        else:
            self.log.debug("config={0}".format(self.config))
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

                self.log.debug("metadata = {0}".format(metadata))
            except:
                self.log.error("Unable to assemble multi-part message.",
                               exc_info=True)
                raise

    def send_data(self, targets, metadata, open_connections, context):

        response = self.config["session"].get(self.source_file)
        try:
            response.raise_for_status()
            self.log.debug("Initiating http get for file '{0}' succeeded."
                           .format(self.source_file))
        except:
            self.log.error("Initiating http get for file '{0}' failed."
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
                self.log.debug("Opening '{0}'...".format(self.target_file))
                file_descriptor = open(self.target_file, "wb")
                file_opened = True
            except IOError as e:
                # errno.ENOENT == "No such file or directory"
                if e.errno == errno.ENOENT:

                    subdir, tmp = os.path.split(metadata["relative_path"])

                    if metadata["relative_path"] in self.config["fix_subdirs"]:
                        self.log.error("Unable to move file '{0}' to '{1}': "
                                       "Directory {2} is not available."
                                       .format(self.source_file,
                                               self.target_file,
                                               metadata["relative_path"]),
                                       exc_info=True)

                    elif subdir in self.config["fix_subdirs"]:
                        self.log.error("Unable to move file '{0}' to '{1}': "
                                       "Directory {2} is not available."
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
                            self.log.info("New target directory created: {0}"
                                          .format(target_path))
                            file_opened = True
                        except OSError as e:
                            self.log.info("Target directory creation failed, "
                                          "was already created in the "
                                          "meantime: {0}"
                                          .format(target_path))
                            file_descriptor = open(self.target_file, "wb")
                            file_opened = True
                        except:
                            self.log.error("Unable to open target file '{0}'."
                                           .format(self.target_file),
                                           exc_info=True)
                            self.log.debug("target_path: {0}"
                                           .format(target_path))
                            raise
                else:
                    self.log.error("Unable to open target file '{0}'."
                                   .format(self.target_file), exc_info=True)
            except:
                self.log.error("Unable to open target file '{0}'."
                               .format(self.target_file), exc_info=True)

        targets_data = [i for i in targets if i[3] == "data"]
        targets_metadata = [i for i in targets if i[3] == "metadata"]
        chunk_number = 0

        self.log.debug("Getting data for file '{0}'..."
                       .format(self.source_file))
        # reading source file into memory
        for data in response.iter_content(chunk_size=chunksize):
            self.log.debug("Packing multipart-message for file '{0}'..."
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
                               "'{0}'".format(self.source_file),
                               exc_info=True)

            if self.config["store_data"]:
                try:
                    file_descriptor.write(data)
                except:
                    self.log.error("Unable write data for file '{0}'"
                                   .format(self.source_file), exc_info=True)
                    file_written = False

            # send message to data targets
            try:
                self.send_to_targets(targets_data, open_connections,
                                     metadata_extended, payload, context)
                self.log.debug("Passing multipart-message for file {0}...done."
                               .format(self.source_file))

            except:
                self.log.error("Unable to send multipart-message for file {0}"
                               .format(self.source_file), exc_info=True)
                file_send = False

            chunk_number += 1

        if self.config["store_data"]:
            try:
                self.log.debug("Closing '{0}'...".format(self.target_file))
                file_descriptor.close()
                file_closed = True
            except:
                self.log.error("Unable to close target file '{0}'."
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
                                     metadata_extended, payload, context)
                self.log.debug("Passing metadata multipart-message for file "
                               "'{0}'...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{0}'".format(self.source_file),
                               exc_info=True)

            self.config["remove_flag"] = (file_opened
                                          and file_written
                                          and file_closed)
        else:
            self.config["remove_flag"] = file_send

    def finish(self, targets, metadata, open_connections, context):

        if self.config["remove_data"] and self.config["remove_flag"]:
            responce = requests.delete(self.source_file)

            try:
                responce.raise_for_status()
                self.log.debug("Deleting file '{0}' succeeded."
                               .format(self.source_file))
            except:
                self.log.error("Deleting file '{0}' failed."
                               .format(self.source_file), exc_info=True)

    def stop(self):
        pass

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
#    import subprocess
    from multiprocessing import Queue
    from logutils.queue import QueueHandler
    from __init__ import BASE_PATH

    logfile = os.path.join(BASE_PATH, "logs", "http_fetcher.log")
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
    logging.debug("=== copy file to asap3-mon")
#    os.system('scp "%s" "%s:%s"' % (localfile, remotehost, remotefile) )
#    subprocess.call("scp {0} root@asap3-mon:/var/www/html/data"
#                    .format(prework_source_file), shell=True)

#    metadata = {
#            "source_path"  : "http://192.168.138.37/data",
#            "relative_path": "",
#            "filename"    : "35_data_000170.h5"
#            }
    metadata = {
        "source_path": "http://asap3-mon/data",
        "relative_path": "",
        "filename": "test_file.cbf"
    }
    targets = [['localhost:{0}'.format(receiving_port), 1, [".cbf", ".tif"],
                "data"],
               ['localhost:{0}'.format(receiving_port2), 1, [".cbf", ".tif"],
                "data"]]

    chunksize = 10485760  # = 1024*1024*10 = 10 MiB
    open_connections = dict()

    config = {
        "session": None,
        "fix_subdirs": ["commissioning", "current", "local"],
        "store_data": True,
        "remove_data": False,
        "chunksize": chunksize,
        "local_target": local_target
    }

    datafetcher = DataFetcher(config, log_queue, 0)

    datafetcher.get_metadata(targets, metadata)
#    source_file = "http://131.169.55.170/test_httpget/data/test_file.cbf"

    datafetcher.send_data(targets, metadata, open_connections, context)

    datafetcher.finish(targets, metadata, open_connections, context)

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
        context.destroy()
