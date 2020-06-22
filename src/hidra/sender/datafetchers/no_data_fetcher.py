# Copyright (C) DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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
This module implements a data fetcher which does not get data but only
distribute the metadata of the files it sees.

No mandatory configuration needed.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
import time

from datafetcherbase import DataFetcherBase
from hidra import generate_filepath

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):
    """
    Implementation of data fetcher to only distribute file metadata.
    """

    def __init__(self, datafetcher_base_config):

        datafetcher_base_config["check_dep"] = False
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

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata to extend.
        """

        # Build source file
        self.source_file = generate_filepath(metadata["source_path"],
                                             metadata)

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
                metadata["chunksize"] = None

                self.log.debug("metadata = %s", metadata)
            except Exception:
                self.log.error("Unable to assemble multi-part message.")
                raise

    def send_data(self, targets, metadata, open_connections):

        if not targets:
            return

        targets_data = [i for i in targets if i[2] == "data"]

        if targets_data:
            self.log.error("Sending data directly is not supported for this "
                           "data fetcher.")

        targets_metadata = [i for i in targets if i[2] == "metadata"]

        if not targets_metadata:
            return

    #    try:
    #        chunksize = metadata["chunksize"]
    #    except:
    #        self.log.error("Unable to get chunksize", exc_info=True)

        try:
            self.log.debug("Packing multipart-message for file %s...",
                           self.source_file)
            chunk_number = 0

            # assemble metadata for zmq-message
            metadata_extended = metadata.copy()
            metadata_extended["chunk_number"] = chunk_number

            payload = [json.dumps(metadata_extended).encode("utf-8"),
                       None]
        except Exception:
            self.log.error("Unable to pack multipart-message for file '%s'",
                           self.source_file, exc_info=True)
            return

        # send message
        try:
            self.send_to_targets(targets=targets_metadata,
                                 open_connections=open_connections,
                                 metadata=metadata_extended,
                                 payload=payload,
                                 chunk_number=None)
            self.log.debug("Passing metadata multipart-message for "
                           "file '%s'...done.", self.source_file)
        except Exception:
            self.log.error("Unable to send metadata multipart-message "
                           "for file '%s'", self.source_file,
                           exc_info=True)

    def finish(self, targets, metadata, open_connections):
        pass

    def stop(self):
        pass
