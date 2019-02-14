from __future__ import print_function
from __future__ import unicode_literals

import json
import time

from datafetcherbase import DataFetcherBase, DataHandlingError
from hidra import generate_filepath, Transfer
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, fetcher_id, context, lock):
        """Initial setup

        Checks if all required parameters are set in the configuration
        """

        DataFetcherBase.__init__(self,
                                 config,
                                 log_queue,
                                 fetcher_id,
                                 "hidra_fetcher-{}".format(fetcher_id),
                                 context,
                                 lock)

        self.config = config
        self.log_queue = log_queue

        self.f_descriptors = dict()
        self.transfer = None
        self.metadata_r = None
        self.data_r = None

        self.set_required_params()

        self.check_config()
        self.setup()

    def set_required_params(self):
        """
        Defines the parameters to be in configuration to run this datafetcher.
        Depending if on Linux or Windows other parameters are required.
        """

        self.required_params = ["context",
                                "store_data",
                                "ext_ip",
                                "status_check_resp_port",
                                "confirmation_resp_port"]

        if utils.is_windows():
            self.required_params += ["data_fetch_port"]
        else:
            self.required_params += ["ipc_dir", "main_pid"]

    def setup(self):
        """
        Sets up and configures the transfer.
        """
        self.transfer = Transfer("STREAM", use_log=self.log_queue)

        endpoint = "{}_{}".format(self.config["main_pid"], "out")
        self.transfer.start([self.config["ipc_dir"], endpoint],
                            protocol="ipc",
                            data_con_style="connect")

        # enable status check requests from any sender
        self.transfer.setopt(option="status_check",
                             value=[self.config["ext_ip"],
                                    self.config["status_check_resp_port"]])

        # enable confirmation reply if this is requested in a received data
        # packet
        self.transfer.setopt(option="confirmation",
                             value=[self.config["ext_ip"],
                                    self.config["confirmation_resp_port"]])

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.
        """

        timeout = 10000

        # Get new data
        self.metadata_r, self.data_r = self.transfer.get(timeout)

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
        """Implementation of the abstract method send_data.
        """

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
            self.send_to_targets(
                targets=targets_data,
                open_connections=open_connections,
                metadata=None,
                payload=chunk_payload,
                chunk_number=self.metadata_r["chunk_number"]
            )
        except DataHandlingError:
            self.log.error(
                "Unable to send multipart-message for file '{}' (chunk {})"
                .format(self.source_file, self.metadata_r["chunk_number"]),
                exc_info=True
            )
        except:
            self.log.error(
                "Unable to send multipart-message for file '{}' (chunk {})"
                .format(self.source_file, self.metadata_r["chunk_number"]),
                exc_info=True
            )

    def finish(self, targets, metadata, open_connections):
        """Implementation of the abstract method finish.
        """

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(
                    targets=targets_metadata,
                    open_connections=open_connections,
                    metadata=metadata,
                    payload=None,
                    chunk_number=None,
                    timeout=self.config["send_timeout"]
                )
                self.log.debug("Passing metadata multipart-message for file "
                               "{}...done.".format(self.source_file))

            except:
                self.log.error(
                    "Unable to send metadata multipart-message for file"
                    "'{}' to '{}'".format(self.source_file, targets_metadata),
                    exc_info=True
                )

        # store data
        if self.config["store_data"]:
            try:
                # TODO: save message to file using a thread (avoids blocking)
                self.transfer.store_data_chunk(
                    descriptors=self.f_descriptors,
                    filepath=self.target_file,
                    payload=self.data_r,
                    base_path=self.config["local_target"],
                    metadata=self.metadata_r
                )
            except:
                self.log.error(
                    "Storing multipart message for file '{}' failed"
                    .format(self.source_file),
                    exc_info=True
                )

    def stop(self):
        """Implementation of the abstract method stop.
        """

        # cloes base class zmq sockets
        self.close_socket()

        # Close open file handler to prevent file corruption
        for target_file in list(self.f_descriptors.keys()):
            self.f_descriptors[target_file].close()
            del self.f_descriptors[target_file]

        # close zmq sockets
        if self.transfer is not None:
            self.transfer.stop()
