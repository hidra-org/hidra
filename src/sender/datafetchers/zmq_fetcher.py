from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import json
import time

from datafetcherbase import DataFetcherBase
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id, context):

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "zmq_fetcher-{}".format(id),
                                 context)

        if utils.is_windows():
            required_params = ["ext_ip",
                               "data_fetcher_port"]
        else:
            required_params = ["ipc_path"]

        # Check format of config
        check_passed, config_reduced = utils.check_config(required_params,
                                                          self.config,
                                                          self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {}"
                          .format(config_reduced))

            if utils.is_windows():
                con_str = ("tcp://{}:{}"
                           .format(self.config["ext_ip"],
                                   self.config["data_fetcher_port"]))
            else:
                con_str = ("ipc://{}/{}"
                           .format(self.config["ipc_path"], "dataFetch"))

            # Create zmq socket
            try:
                self.socket = self.context.socket(zmq.PULL)
                self.socket.bind(con_str)
                self.log.info("Start socket (bind): '{}'".format(con_str))
            except:
                self.log.error("Failed to start com_socket (bind): '{}'"
                               .format(con_str), exc_info=True)
                raise
        else:
            # self.log.debug("config={}".format(self.config))
            raise Exception("Wrong configuration")

    def get_metadata(self, targets, metadata):

        # extract event metadata
        try:
            # TODO validate metadata dict
            self.source_file = metadata["filename"]
        except:
            self.log.error("Invalid fileEvent message received.",
                           exc_info=True)
            self.log.debug("metadata={}".format(metadata))
            # skip all further instructions and continue with next iteration
            raise

        # TODO combine better with source_file... (for efficiency)
        if self.config["local_target"]:
            self.target_file = os.path.join(self.config["local_target"],
                                            self.source_file)
        else:
            self.target_file = None

        if targets:
            try:
                self.log.debug("create metadata for source file...")
                # metadata = {
                #        "filename"       : ...,
                #        "file_mod_time"    : ...,
                #        "file_create_time" : ...,
                #        "chunksize"      : ...
                #        }
                metadata["filesize"] = None
                metadata["file_mod_time"] = time.time()
                metadata["file_create_time"] = time.time()
                # chunksize is coming from zmq_events

                self.log.debug("metadata = {}".format(metadata))
            except:
                self.log.error("Unable to assemble multi-part message.",
                               exc_info=True)
                raise

    def send_data(self, targets, metadata, open_connections):

        if not targets:
            return

        # reading source file into memory
        try:
            self.log.debug("Getting data out of queue for file '{}'..."
                           .format(self.source_file))
            data = self.socket.recv()
        except:
            self.log.error("Unable to get data out of queue for file '{}'"
                           .format(self.source_file), exc_info=True)
            raise

    #    try:
    #        chunksize = metadata["chunksize"]
    #    except:
    #        self.log.error("Unable to get chunksize", exc_info=True)

        try:
            self.log.debug("Packing multipart-message for file {}..."
                           .format(self.source_file))
            chunk_number = 0

            # assemble metadata for zmq-message
            metadata_extended = metadata.copy()
            metadata_extended["chunk_number"] = chunk_number

            payload = []
            payload.append(json.dumps(metadata_extended).encode("utf-8"))
            payload.append(data)
        except:
            self.log.error("Unable to pack multipart-message for file '{}'"
                           .format(self.source_file), exc_info=True)

        # send message
        try:
            self.send_to_targets(targets, open_connections, metadata_extended,
                                 payload)
            self.log.debug("Passing multipart-message for file '{}'...done."
                           .format(self.source_file))
        except:
            self.log.error("Unable to send multipart-message for file '{}'"
                           .format(self.source_file), exc_info=True)

    def finish(self, targets, metadata, open_connections):
        pass

    def stop(self):
        # Close zmq socket
        if self.socket is not None:
            self.socket.close(0)
            self.socket = None

# testing was moved into test/unittests/datafetchers/test_zmq_fetcher.py
