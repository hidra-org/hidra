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
This module implements the data fetcher used for the Eiger detector and other
detectors with a http interface.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import errno
import json
import os
import time

import requests

from cleanerbase import CleanerBase
from datafetcherbase import DataFetcherBase
from hidra import generate_filepath

__author__ = ('Manuela Kuhn <manuela.kuhn@desy.de>',
              'Jan Garrevoet <jan.garrevoet@desy.de>')


class DataFetcher(DataFetcherBase):
    """
    Implemenation of the data fetcher to get files from the Eiger detector or
    other detectors with a http interface.
    """

    def __init__(self, config, log_queue, fetcher_id, context):

        DataFetcherBase.__init__(self,
                                 config,
                                 log_queue,
                                 fetcher_id,
                                 "http_fetcher-{}".format(fetcher_id),
                                 context)

        self.required_params = ["session",
                                "store_data",
                                "remove_data",
                                "fix_subdirs"]

        self.check_config()
        self.setup()

    def setup(self):
        """
        Sets static configuration parameters and which finish method to use.
        """

        self.config["session"] = requests.session()
        self.config["remove_flag"] = False

        if self.config["remove_data"] == "with_confirmation":
            self.finish = self.finish_with_cleaner
        else:
            self.finish = self.finish_without_cleaner

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata to extend.
        """

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
                    self.config["remove_data"] == "with_confirmation"
                )

                self.log.debug("metadata = {}".format(metadata))
            except:
                self.log.error("Unable to assemble multi-part message.",
                               exc_info=True)
                raise

    def create_directory(self, metadata):
        """Creates the directory where the file should be stored.

        Args:
            metadata (dict): The file metadata for the directory to be created.

        """
        # the directories current, commissioning and local should
        # not be created
        if metadata["relative_path"] in self.config["fix_subdirs"]:
            msg = (
                "Unable to move file '{}' to '{}': Directory {} is not "
                "available.".format(self.source_file,
                                    self.target_file,
                                    metadata["relative_path"])
            )
            self.log.error(msg, exc_info=True)
            raise Exception(msg)
        else:
            fix_subdir_found = False
            # identify which of the pefixes is the correct one and check that
            # this is available
            # e.g. relative_path is commissioning/raw/test_dir but
            #      commissioning/raw  does not exist, it should not be created
            for prefix in self.config["fix_subdirs"]:
                if metadata["relative_path"].startswith(prefix):
                    fix_subdir_found = True

                    prefix_dir = os.path.join(self.config["local_target"],
                                              prefix)
                    if not os.path.exists(prefix_dir):
                        msg = (
                            "Unable to move file '{}' to '{}': Directory {} "
                            "is not available.".format(self.source_file,
                                                       self.target_file,
                                                       prefix)
                        )
                        self.log.error(msg, exc_info=True)
                        raise Exception(msg)
                    else:
                        # everything is fine -> create directory
                        try:
                            target_path, _ = os.path.split(self.target_file)
                            os.makedirs(target_path)
                            self.log.info("New target directory created: {}"
                                          .format(target_path))
                        except OSError:
                            self.log.info("Target directory creation failed, "
                                          "was already created in the "
                                          "meantime: {}"
                                          .format(target_path))
                        except Exception:
                            self.log.error("Unable to create target directory "
                                           "'{}'."
                                           .format(target_path), exc_info=True)
                            raise

                        break

            if not fix_subdir_found:
                raise Exception("Relative path was not found in fix_subdir")

    def send_data(self, targets, metadata, open_connections):
        """Implementation of the abstract method send_data.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
        """

        response = self.config["session"].get(self.source_file)
        try:
            response.raise_for_status()
            self.log.debug("Initiating http get for file '{}' succeeded."
                           .format(self.source_file))
        except Exception:
            self.log.error("Initiating http get for file '{}' failed."
                           .format(self.source_file), exc_info=True)
            return

        try:
            chunksize = metadata["chunksize"]
        except Exception:
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
            except IOError as excp:
                err_msg = ("Unable to open target file '{}'."
                           .format(self.target_file))

                # errno.ENOENT == "No such file or directory"
                if excp.errno == errno.ENOENT:
                    self.create_directory(metadata)

                    try:
                        file_descriptor = open(self.target_file, "wb")
                        file_opened = True
                    except:
                        self.log.error(err_msg, exc_info=True)
                        raise
                else:
                    self.log.error(err_msg, exc_info=True)
                    raise
            except:
                self.log.error(err_msg, exc_info=True)
                raise

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
            except Exception:
                self.log.error("Unable to pack multipart-message for file "
                               "'{}'".format(self.source_file),
                               exc_info=True)

            if self.config["store_data"] and file_opened:
                try:
                    file_descriptor.write(data)
                    self.log.debug("Writing data for file '{}' (chunk {})"
                                   .format(self.source_file, chunk_number))
                except Exception:
                    self.log.error("Unable write data for file '{}'"
                                   .format(self.source_file), exc_info=True)
                    file_written = False

            if targets_data != []:
                # send message to data targets
                try:
                    self.send_to_targets(targets_data,
                                         open_connections,
                                         metadata_extended,
                                         payload)
                    msg = ("Passing multipart-message for file {}...done."
                           .format(self.source_file))
                    self.log.debug(msg)

                except Exception:
                    msg = ("Unable to send multipart-message for file {}"
                           .format(self.source_file))
                    self.log.error(msg, exc_info=True)
                    file_send = False

            chunk_number += 1

        if self.config["store_data"] and file_opened:
            try:
                self.log.debug("Closing '{}'...".format(self.target_file))
                file_descriptor.close()
                file_closed = True
            except Exception:
                self.log.error("Unable to close target file '{}'."
                               .format(self.target_file), exc_info=True)
                raise

            # update the creation and modification time
            metadata_extended["file_mod_time"] = (
                os.stat(self.target_file).st_mtime)
            metadata_extended["file_create_time"] = (
                os.stat(self.target_file).st_ctime)

            if targets_metadata != []:
                # send message to metadata targets
                try:
                    self.send_to_targets(targets_metadata,
                                         open_connections,
                                         metadata_extended,
                                         payload)
                    self.log.debug("Passing metadata multipart-message for "
                                   "file '{}'...done."
                                   .format(self.source_file))

                except Exception:
                    self.log.error("Unable to send metadata multipart-message "
                                   "for file '{}'".format(self.source_file),
                                   exc_info=True)

            self.config["remove_flag"] = (file_opened
                                          and file_written
                                          and file_closed)
        else:
            self.config["remove_flag"] = file_send

    # pylint: disable=method-hidden
    def finish(self, targets, metadata, open_connections):
        """Implementation of the abstract method finish.

        Is overwritten when class is instantiated depending if a cleaner class
        is used or not

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
        """
        pass

    def finish_with_cleaner(self, targets, metadata, open_connections):
        """Finish method to be used if use of cleaner was configured.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
        """
         # pylint: disable=unused-argument

        file_id = self.generate_file_id(metadata)

        self.cleaner_job_socket.send_multipart(
            [metadata["source_path"].encode("utf-8"),
             file_id.encode("utf-8")])
        self.log.debug("Forwarded to cleaner {}".format(file_id))

    def finish_without_cleaner(self, targets, metadata, open_connections):
        """Finish method to use when use of cleaner not configured.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
        """
         # pylint: disable=unused-argument

        if self.config["remove_data"] and self.config["remove_flag"]:
            responce = requests.delete(self.source_file)

            try:
                responce.raise_for_status()
                self.log.debug("Deleting file '{}' succeeded."
                               .format(self.source_file))
            except Exception:
                self.log.error("Deleting file '{}' failed."
                               .format(self.source_file), exc_info=True)

    def stop(self):
        """Implementation of the abstract method stop.
        """

        # cloes base class zmq sockets
        self.close_socket()


class Cleaner(CleanerBase):
    """
    Implementation of the cleaner when handling http detectors.
    """

    def remove_element(self, base_path, file_id):

        # generate file path
        source_file = os.path.join(base_path, file_id)

        # remove file
        responce = requests.delete(source_file)

        try:
            responce.raise_for_status()
            self.log.debug("Deleting file '{}' succeeded."
                           .format(source_file))
        except Exception:
            self.log.error("Deleting file '{}' failed."
                           .format(source_file), exc_info=True)
