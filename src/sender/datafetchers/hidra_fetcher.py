# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""
This module implements a data fetcher to connect multiple hidra instances
in series.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
import time

from datafetcherbase import DataFetcherBase, DataHandlingError
from hidra import generate_filepath, Transfer
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):
    """
    Implementation of the data fetcher reacting on data sent by another
    hidra instance.
    """

    def __init__(self, config, log_queue, fetcher_id, context):
        """Initial setup

        Checks if all required parameters are set in the configuration
        """

        DataFetcherBase.__init__(self,
                                 config,
                                 log_queue,
                                 fetcher_id,
                                 "hidra_fetcher-{}".format(fetcher_id),
                                 context)


        # base class sets
        #   self.config_all - all configurations
        #   self.config_df - the config of the datafetcher
        #   self.config - the module specific config
        #   self.df_type -  the name of the datafetcher module
        #   self.log_queue
        #   self.log

        self.f_descriptors = dict()
        self.transfer = None
        self.metadata_r = None
        self.data_r = None

        self.set_required_params()

        # check that the required_params are set inside of module specific
        # config
        self.check_config()

        self._setup()

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

    def _setup(self):
        """Sets up and configures the transfer.
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

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata to extend.
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

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
        """

        if not targets:
            return

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_data = [i for i in targets if i[2] == "data"]

        if not targets_data:
            return

        self.log.debug("Received data for file %s (chunknumber %s)",
                       self.source_file, self.metadata_r["chunk_number"])

        self.log.debug("Passing multipart-message for file '%s'...",
                       self.source_file)

        try:
            chunk_payload = [json.dumps(self.metadata_r).encode("utf-8"),
                             self.data_r]
        except Exception:
            self.log.error("Unable to pack multipart-message for file "
                           "'%s'", self.source_file, exc_info=True)

        # send message to data targets
        try:
            self.send_to_targets(
                targets=targets_data,
                open_connections=open_connections,
                metadata=None,
                payload=chunk_payload
            )
        except DataHandlingError:
            self.log.error(
                "Unable to send multipart-message for file '%s' (chunk %s)",
                self.source_file, self.metadata_r["chunk_number"],
                exc_info=True
            )
        except Exception:
            self.log.error(
                "Unable to send multipart-message for file '%s' (chunk %s)",
                self.source_file, self.metadata_r["chunk_number"],
                exc_info=True
            )

    def finish(self, targets, metadata, open_connections):
        """Implementation of the abstract method finish.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
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
                    timeout=self.config["send_timeout"]
                )
                self.log.debug("Passing metadata multipart-message for file "
                               "%s...done.", self.source_file)

            except Exception:
                self.log.error(
                    "Unable to send metadata multipart-message for file"
                    "'%s' to '%s'", self.source_file, targets_metadata,
                    exc_info=True
                )

        # store data
        if self.config["store_data"]:
            try:
                # TODO: save message to file using a thread (avoids blocking)
                self.transfer.store_chunk(
                    descriptors=self.f_descriptors,
                    filepath=self.target_file,
                    payload=self.data_r,
                    base_path=self.config["local_target"],
                    metadata=self.metadata_r
                )
            except Exception:
                self.log.error(
                    "Storing multipart message for file '%s' failed",
                    self.source_file,
                    exc_info=True
                )

    def stop(self):
        """Implementation of the abstract method stop.
        """

        # cloes base class zmq sockets
        self.close_socket()

        # Close open file handler to prevent file corruption
        for target_file in list(self.f_descriptors):
            self.f_descriptors[target_file].close()
            del self.f_descriptors[target_file]

        # close zmq sockets
        if self.transfer is not None:
            self.transfer.stop()
