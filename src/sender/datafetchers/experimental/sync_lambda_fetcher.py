# Copyright (C) 2019  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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
This module implements a data fetcher to pass through synchronized images.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple
import json
import time
import os
import zmq

from datafetcherbase import DataFetcherBase
from hidra import generate_filepath
import hidra.utils as utils
from cleanerbase import CleanerBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):
    """
    Implementation of the data fetcher to be used with the ingest API.
    """

    def __init__(self, datafetcher_base_config):

        DataFetcherBase.__init__(self, datafetcher_base_config,
                                 name=__name__)

        # base class sets
        #   self.config_all - all configurations
        #   self.config_df - the config of the datafetcher
        #   self.config - the module specific config
        #   self.df_type -  the name of the datafetcher module
        #   self.log_queue
        #   self.log

        self.ipc_addresses = None
        self.endpoints = None

        self.source_file = None

        if utils.is_windows():
            self.required_params = {
                "network": ["ext_ip"],
                "datafetcher": {
                    self.df_type: ["internal_com_endpoint"]
                }
            }
        else:
            self.required_params = {
                "network": ["ipc_dir"],
                "datafetcher": {
                    self.df_type: ["internal_com_endpoint"]
                }
            }

        self._setup()

    def _setup(self):
        """
        Sets ZMQ endpoints and addresses and creates the ZMQ socket.
        """

        # check that the required_params are set inside of module specific
        # config
        self.check_config()

        # Create zmq socket
        self.socket = self.start_socket(
            name="socket",
            sock_type=zmq.PULL,
            sock_con="connect",
            endpoint=self.config["internal_com_endpoint"]
        )

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.
        """

        # pylint: disable=attribute-defined-outside-init

        # Build source file
        self.source_file = metadata["filename"]

        if not targets:
            return

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
            metadata["chunksize"] = None

            self.log.debug("metadata = %s", metadata)
        except Exception:
            self.log.error("Unable to assemble multi-part message.",
                           exc_info=True)
            raise

    def send_data(self, targets, metadata, open_connections):
        """Implementation of the abstract method send_data.
        """

        if not targets:
            return

        # reading source file into memory
        try:
            self.log.debug("Getting data for file '%s'...", self.source_file)
            recv_msg = self.socket.recv()
        except Exception:
            self.log.error("Unable to get data for file '%s'",
                           self.source_file, exc_info=True)
            raise

        # only for testing
        data = [
            utils.zmq_msg_to_nparray(
                data=msg,
                array_metadata=metadata["additional_info"][i]
            )
            for i, msg in enumerate(recv_msg)
        ]
        self.log.debug("data=%s", data)

        try:
            self.log.debug("Packing multipart-message for file %s...",
                           self.source_file)
            chunk_number = 0

            # assemble metadata for zmq-message
            metadata_extended = metadata.copy()
            metadata_extended["chunk_number"] = chunk_number

            payload = [json.dumps(metadata_extended).encode("utf-8")] + data
            #payload = [json.dumps(metadata_extended).encode("utf-8"), data]
        except Exception:
            self.log.error("Unable to pack multipart-message for file '%s'",
                           self.source_file, exc_info=True)
            return

        # send message
        try:
            self.send_to_targets(targets=targets,
                                 open_connections=open_connections,
                                 metadata=metadata_extended,
                                 payload=payload,
                                 chunk_number=chunk_number)
            self.log.debug("Passing multipart-message for file '%s'...done.",
                           self.source_file)
        except Exception:
            self.log.error("Unable to send multipart-message for file '%s'",
                           self.source_file, exc_info=True)

    def finish(self, targets, metadata, open_connections):
        """Implementation of the abstract method finish.
        """
        pass

    def stop(self):
        """Implementation of the abstract method stop.
        """

        # close base class zmq sockets
        self.close_socket()

        # Close zmq socket
        self.stop_socket(name="socket")


class Cleaner(CleanerBase):
    def remove_element(self, base_path, file_id):
        pass
