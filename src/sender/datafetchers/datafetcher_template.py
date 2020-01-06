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
This is a template module for implementing a data fetchers.

No mandatory configuration needed.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json

from datafetcherbase import DataFetcherBase, DataHandlingError
from hidra import generate_filepath

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):
    """
    Implementation of the data fetcher.
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

        self.required_params = []

        # check that the required_params are set inside of module specific
        # config
        self.check_config()

        self._setup()

    def _setup(self):
        """Sets static configuration parameters.
        """
        pass

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata to extend.
        """
        # pylint: disable=attribute-defined-outside-init

        # Build source file
        self.source_file = generate_filepath(metadata["source_path"],
                                             metadata)

        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config_df["local_target"],
                                             metadata)

        # Extends metadata
        if targets:
            metadata["filesize"] = 0
            metadata["file_mod_time"] = 1481734310.6207027
            metadata["file_create_time"] = 1481734310.6207028
            metadata["chunksize"] = self.config_df["chunksize"]

    def send_data(self, targets, metadata, open_connections):
        """Implementation of the abstract method send_data.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
        """

        if not targets:
            return

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_data = [i for i in targets if i[2] == "data"]

        if not targets_data:
            return

        self.log.debug("Passing multipart-message for file '%s'...",
                       self.source_file)

        for i in range(5):

            chunk_number = i
            file_content = "test_data_{}".format(chunk_number).encode("ascii")

            try:
                # assemble metadata for zmq-message
                chunk_metadata = metadata.copy()
                chunk_metadata["chunk_number"] = chunk_number

                chunk_payload = [json.dumps(chunk_metadata).encode("utf-8"),
                                 file_content]
            except Exception:
                self.log.error("Unable to pack multipart-message for file "
                               "'%s'", self.source_file, exc_info=True)
                continue

            # send message to data targets
            try:
                self.send_to_targets(targets=targets_data,
                                     open_connections=open_connections,
                                     metadata=None,
                                     payload=chunk_payload,
                                     chunk_number=chunk_number)
            except DataHandlingError:
                self.log.error("Unable to send multipart-message for file "
                               "'%s' (chunk %s)", self.source_file,
                               chunk_number, exc_info=True)
            except Exception:
                self.log.error("Unable to send multipart-message for file "
                               "'%s' (chunk %s)", self.source_file,
                               chunk_number, exc_info=True)

    def finish(self, targets, metadata, open_connections):
        """Implementation of the abstract method finish.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
        """

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(targets=targets_metadata,
                                     open_connections=open_connections,
                                     metadata=metadata,
                                     payload=None,
                                     chunk_number=None)
                self.log.debug("Passing metadata multipart-message for file "
                               "%s...done.", self.source_file)

            except Exception:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '%s' to '%s'", self.source_file,
                               targets_metadata, exc_info=True)

    def stop(self):
        """Implementation of the abstract method stop.
        """
        pass
