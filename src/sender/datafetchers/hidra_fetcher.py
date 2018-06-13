from __future__ import print_function
from __future__ import unicode_literals

import zmq
import json
import time

from datafetcherbase import DataFetcherBase, DataHandlingError
from hidra import generate_filepath, Transfer
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, id, context):
        """Initial setup

        Checks if all required parameters are set in the configuration
        """

        DataFetcherBase.__init__(self, config, log_queue, id,
                                 "hidra_fetcher-{}".format(id),
                                 context)

        self.f_descriptors = dict()
        self.transfer = None

        required_params = ["context",
                           "store_data",
                           "ext_ip",
                           "status_check_resp_port",
                           "confirmation_resp_port"]

        if utils.is_windows():
            required_params += ["data_fetch_port"]
        else:
            required_params += ["ipc_path",
                                "main_pid"]

        # Check format of config
        check_passed, config_reduced = utils.check_config(required_params,
                                                          self.config,
                                                          self.log)

        context = zmq.Context()

        if check_passed:
            self.log.info("Configuration for data fetcher: {}"
                          .format(config_reduced))

            self.metadata_r = None
            self.data_r = None

            self.transfer = Transfer("STREAM", use_log=log_queue)

            self.transfer.start([self.config["ipc_path"],
                                 "{}_{}".format(self.config["main_pid"],
                                                "out")],
                                protocol="ipc", data_con_style="connect")

            # enable status check requests from any sender
            self.transfer.setopt("status_check",
                                 [self.config["ext_ip"],
                                  self.config["status_check_resp_port"]])

            # enable confirmation reply if this is requested in a received data
            # packet
            self.transfer.setopt("confirmation",
                                 [self.config["ext_ip"],
                                  self.config["confirmation_resp_port"]])

        else:
            # self.log.debug("config={}".format(self.config))
            raise Exception("Wrong configuration")

    def get_metadata(self, targets, metadata):

        # Get new data
        self.metadata_r, self.data_r = self.transfer.get()

        if (metadata["relative_path"] != self.metadata_r["relative_path"]
                or metadata["source_path"] != self.metadata_r["source_path"]
                or metadata["filename"] != self.metadata_r["filename"]):
            self.log.error("Received metadata do not match data")

        # Use received data to prevent missmatch of metadata and data
        # TODO handle case if file type requesed by target does not match

        # Build source file
        self.source_file = generate_filepath(self.metadata_r["source_path"],
                                             self.metadata_r)

        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config["local_target"],
                                             self.metadata_r)

        # Extends metadata
        if targets:
            if "filesize" not in self.metadata_r:
                self.log.error("Received metadata do not contain 'filesize'")
            if "file_mod_time" not in self.metadata_r:
                self.log.error("Received metadata do not contain "
                               "'file_mod_time'. Setting it to current time")
                self.metadata_r["file_mod_time"] = time.time()
            if "file_create_time" not in self.metadata_r:
                self.log.error("Received metadata do not contain "
                               "'file_create_time'. Setting it to current "
                               "time")
                self.metadata_r["file_create_time"] = time.time()
            if "chunksize" not in self.metadata_r:
                self.log.error("Received metadata do not contain 'chunksize'. "
                               "Setting it to locally configured one")
                self.metadata_r["chunksize"] = self.config["chunksize"]

    def send_data(self, targets, metadata, open_connections):

        if not targets:
            return

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_data = [i for i in targets if i[2] == "data"]

        if not targets_data:
            return

        self.log.debug("Received data for file {} (chunknumber {})"
                       .format(self.source_file,
                               self.metadata_r["chunk_number"]))

        self.log.debug("Passing multipart-message for file '{}'..."
                       .format(self.source_file))

        try:
            chunk_payload = [json.dumps(self.metadata_r).encode("utf-8"),
                             self.data_r]
        except:
            self.log.error("Unable to pack multipart-message for file "
                           "'{}'".format(self.source_file),
                           exc_info=True)

        # send message to data targets
        try:
            self.send_to_targets(targets_data, open_connections, None,
                                 chunk_payload)
        except DataHandlingError:
            self.log.error("Unable to send multipart-message for file "
                           "'{}' (chunk {})"
                           .format(self.source_file,
                                   self.metadata_r["chunk_number"]),
                           exc_info=True)
        except:
            self.log.error("Unable to send multipart-message for file "
                           "'{}' (chunk {})"
                           .format(self.source_file,
                                   self.metadata_r["chunk_number"]),
                           exc_info=True)

    def finish(self, targets, metadata, open_connections):

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(targets_metadata, open_connections,
                                     metadata, None,
                                     self.config["send_timeout"])
                self.log.debug("Passing metadata multipart-message for file "
                               "{}...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{}' to '{}'"
                               .format(self.source_file, targets_metadata),
                               exc_info=True)

        if self.config["store_data"]:
            # store data
            try:
                # TODO: save message to file using a thread (avoids blocking)
                self.transfer.store_data_chunk(self.f_descriptors,
                                               self.target_file,
                                               self.data_r,
                                               self.config["local_target"],
                                               self.metadata_r)
            except:
                self.log.error("Storing multipart message for file '{}' "
                               "failed".format(self.source_file),
                               exc_info=True)

    def stop(self):

        # Close open file handler to prevent file corruption
        for target_file in list(self.f_descriptors.keys()):
            self.f_descriptors[target_file].close()
            del self.f_descriptors[target_file]

        # close zmq sockets
        if self.transfer is not None:
            self.transfer.stop()

# testing was moved into test/unittests/datafetchers/test_hidra_fetcher.py
