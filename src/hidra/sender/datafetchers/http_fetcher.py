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

Needed configuration in config file:
datafetcher:
    type: http_fetcher
    http_fetcher:
        fix_subdirs: list of strings

Example config:
    http_fetcher:
        fix_subdirs:
            - "commissioning/raw"
            - "commissioning/scratch_bl"
            - "current/raw"
            - "current/scratch_bl"
            - "local"
"""

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


class Filewriter(object):
    def __init__(self, file_id, target_file, config, config_df, log):
        self.file_id = file_id
        self.target_file = target_file
        self.config = config
        self.config_df = config_df
        self.log = log

        self.descriptor = None
        self.status = {
            "opened": False,
            "written_without_error": True,
            "closed": False
        }
        self.writing_enabled = self.config_df["store_data"]

    def open(self, metadata):
        """Open a file descriptor to the file"""
        if not self.writing_enabled:
            return

        try:
            self.log.debug("Opening '%s'...", self.target_file)
            self.descriptor = open(self.target_file, "wb")
            self.status["opened"] = True
        except IOError as excp:
            err_msg = ("Unable to open target file '%s'.", self.target_file)

            # errno.ENOENT == "No such file or directory"
            if excp.errno == errno.ENOENT:
                self.create_directory(metadata)

                try:
                    self.descriptor = open(self.target_file, "wb")
                    self.status["opened"] = True
                except Exception:
                    self.log.error(err_msg, exc_info=True)
                    raise
            else:
                self.log.error(err_msg, exc_info=True)
                raise
        except Exception:
            self.log.error("Unable to open target file '%s'", self.target_file,
                           exc_info=True)
            raise

    def write(self, data, chunk_number):
        """Write the data to the filesystem"""

        if not self.writing_enabled:
            return

        try:
            self.descriptor.write(data)
            self.log.debug("Writing data for file '%s' (chunk %s)",
                           self.file_id, chunk_number)
        except AttributeError:
            self.log.error("Writing failed due to missing file descriptor")
            self.status["written_without_error"] = False
        except Exception:
            self.log.error("Unable write data for file '%s'",
                           self.file_id, exc_info=True)
            self.status["written_without_error"] = False

    def close(self):
        """Close the file descriptor"""

        if not self.writing_enabled or not self.status["opened"]:
            return

        try:
            self.log.debug("Closing '%s'...", self.target_file)
            self.descriptor.close()
            self.status["closed"] = True
        except Exception:
            self.log.error("Unable to close target file '%s'.",
                           self.target_file, exc_info=True)
            raise

    def mark_as_failed(self):
        """Mark that an external problem has occurred"""
        self.status["written_without_error"] = False

    def was_successful(self):
        """Checks if the writing was successful"""
        return (self.status["opened"]
                and self.status["written_without_error"]
                and self.status["closed"])

    def create_directory(self, metadata):
        """Creates the directory where the file should be stored.

        Args:
            metadata (dict): The file metadata for the directory to be created.

        """
        # the directories current, commissioning and local should
        # not be created
        if metadata["relative_path"] in self.config["fix_subdirs"]:
            msg = (
                "Unable to move file '%s' to '%s': Directory %s is not "
                "available.", self.file_id, self.target_file,
                metadata["relative_path"]
            )
            self.log.error(*msg, exc_info=True)
            raise Exception(msg[0] % msg[1:])

        fix_subdir_found = False
        # identify which of the prefixes is the correct one and check that
        # this is available
        # e.g. relative_path is commissioning/raw/test_dir but
        #      commissioning/raw  does not exist, it should not be created
        for prefix in self.config["fix_subdirs"]:
            if not metadata["relative_path"].startswith(prefix):
                continue

            fix_subdir_found = True

            prefix_dir = os.path.join(self.config_df["local_target"], prefix)
            if not os.path.exists(prefix_dir):
                msg = ("Unable to move file '%s' to '%s': Directory %s is not "
                       "available.", self.file_id, self.target_file, prefix)
                self.log.error(*msg, exc_info=True)
                raise Exception(msg[0] % msg[1:])

            target_path, _ = os.path.split(self.target_file)

            # everything is fine -> create directory
            try:
                os.makedirs(target_path)
                self.log.info("New target directory created: %s", target_path)
            except OSError:
                self.log.info("Target directory creation failed, was already "
                              "created in the meantime: %s", target_path)
            except Exception:
                self.log.error("Unable to create target directory '%s'.",
                               target_path, exc_info=True)
                raise

            break

        if not fix_subdir_found:
            raise Exception("Relative path was not found in fix_subdir")


class DataFetcher(DataFetcherBase):
    """
    Implementation of the data fetcher to get files from the Eiger detector or
    other detectors with a http interface.
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

        self.required_params = {
            "datafetcher": [
                "store_data",
                "remove_data",
                {
                    self.df_type: {
                        "fix_subdirs"
                    }
                }
            ],
            "network": ["session"]
        }

        # check that the required_params are set inside of module specific
        # config
        self.check_config()
        self.setup()

    def setup(self):
        """
        Sets static configuration parameters and which finish method to use.
        """

        self.config["session"] = requests.session()
        self.config["remove_flag"] = False

        if self.config_df["use_cleaner"]:
            self.finish = self.finish_with_cleaner
        else:
            self.finish = self.finish_without_cleaner

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata to extend.
        """

        # pylint: disable=attribute-defined-outside-init

        # no normpath used because that would transform http://...
        # into http:/...
        self.source_file = os.path.join(metadata["source_path"],
                                        metadata["relative_path"],
                                        metadata["filename"])

        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config_df["local_target"],
                                             metadata)

        metadata["chunksize"] = self.config_df["chunksize"]

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
                if self.config_df["remove_data"] == "with_confirmation":
                    metadata["confirmation_required"] = (
                        self.confirmation_topic.decode()
                    )
                else:
                    metadata["confirmation_required"] = False

                self.log.debug("metadata = %s", metadata)
            except Exception:
                self.log.error("Unable to assemble multi-part message.",
                               exc_info=True)
                raise

    def send_data(self, targets, metadata, open_connections):
        """Implementation of the abstract method send_data.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata of the file
            open_connections (dict): The dictionary containing all open zmq
                connections.
        """

        response = self.config["session"].get(self.source_file, stream=True)
        try:
            response.raise_for_status()
            self.log.debug("Initiating http get for file '%s' succeeded.",
                           self.source_file)
        except Exception:
            self.log.error("Initiating http get for file '%s' failed.",
                           self.source_file, exc_info=True)
            return

        try:
            chunksize = metadata["chunksize"]
        except Exception:
            self.log.error("Unable to get chunksize", exc_info=True)
            raise

        writer = Filewriter(file_id=self.source_file,
                            target_file=self.target_file,
                            config=self.config,
                            config_df=self.config_df,
                            log=self.log)

        sending_failed = False
        self.config["remove_flag"] = False

        writer.open(metadata)

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_data = [i for i in targets if i[2] == "data"]
        targets_metadata = [i for i in targets if i[2] == "metadata"]
        chunk_number = 0

        self.log.debug("Getting data for file '%s'...", self.source_file)
        # reading source file into memory
        try:
            for data in response.iter_content(chunk_size=chunksize):

                # the datadispatcher asked to stop
                if self.stop_request.is_set():
                    writer.mark_as_failed()
                    self.log.warning("Abort data saving. This might result in "
                                     "missing data.")
                    return

                if not data:
                    continue

                writer.write(data, chunk_number)

                metadata_ext, payload = self._get_prep_data(
                    metadata=metadata,
                    chunk_number=chunk_number,
                    data=data
                )

                # send message to data targets
                sending_failed = sending_failed or self._send_to_targets(
                    targets=targets_data,
                    open_connections=open_connections,
                    metadata=metadata_ext,
                    payload=payload,
                    chunk_number=chunk_number
                )
                chunk_number += 1
        finally:
            # to make sure that the files are not corrupted even when hidra
            # is stopped
            writer.close()

        if self.config_df["store_data"]:
            # send message to metadata targets
            self._send_to_targets(targets=targets_metadata,
                                  open_connections=open_connections,
                                  metadata=metadata_ext,
                                  payload=None,
                                  chunk_number=None,
                                  message_type="metadata ")

            self.config["remove_flag"] = writer.was_successful()
        else:
            self.config["remove_flag"] = not sending_failed

    def _get_prep_data(self, metadata, chunk_number, data):
        try:
            self.log.debug("Packing multipart-message for file '%s'...",
                           self.source_file)

            # assemble metadata for zmq-message
            metadata_extended = metadata.copy()
            metadata_extended["chunk_number"] = chunk_number

            payload = [json.dumps(metadata_extended).encode("utf-8"), data]
        except Exception:
            self.log.error("Unable to pack multipart-message for file '%s'",
                           self.source_file, exc_info=True)

        return metadata_extended, payload

    def _send_to_targets(self, targets, open_connections, metadata, payload,
                         chunk_number, message_type=""):

        # TODO what can target be? ([], None,...?)
        if not targets != []:
            return

        if message_type == "metadata":
            # update the creation and modification time
            metadata["file_mod_time"] = os.stat(self.target_file).st_mtime
            metadata["file_create_time"] = os.stat(self.target_file).st_ctime

        # wrapper around send_to_targets
        try:
            self.send_to_targets(targets=targets,
                                 open_connections=open_connections,
                                 metadata=metadata,
                                 payload=payload,
                                 chunk_number=chunk_number)
            self.log.debug("Passing %smultipart-message for file %s...done.",
                           message_type, self.source_file)
            sending_failed = False
        except Exception:
            self.log.debug("targets=%s", targets)
            self.log.debug("open_connections=%s", open_connections)
            self.log.error("Unable to send %smultipart-message for file %s",
                           message_type, self.source_file, exc_info=True)
            sending_failed = True

        return sending_failed

    # pylint: disable=method-hidden
    def finish(self, targets, metadata, open_connections):
        """Implementation of the abstract method finish.

        Is overwritten when class is instantiated depending if a cleaner class
        is used or not

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata of the file
            open_connections (dict): The dictionary containing all open zmq
                connections.
        """
        pass

    def finish_with_cleaner(self, targets, metadata, open_connections):
        """Finish method to be used if use of cleaner was configured.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata of the file
            open_connections (dict): The dictionary containing all open zmq
                connections.
        """
        # pylint: disable=unused-argument

        file_id = self.generate_file_id(metadata)
        n_chunks = 1

        self.cleaner_job_socket.send_multipart([
            metadata["source_path"].encode("utf-8"),
            file_id.encode("utf-8"),
            str(n_chunks).encode("utf-8")
        ])
        self.log.debug("Forwarded to cleaner %s", file_id)

    def finish_without_cleaner(self, targets, metadata, open_connections):
        """Finish method to use when use of cleaner not configured.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata of the file
            open_connections (dict): The dictionary containing all open zmq
                connections.
        """
        # pylint: disable=unused-argument

        if self.config_df["remove_data"] and self.config["remove_flag"]:
            response = requests.delete(self.source_file)

            try:
                response.raise_for_status()
                self.log.debug("Deleting file '%s' succeeded.",
                               self.source_file)
            except Exception:
                self.log.error("Deleting file '%s' failed.", self.source_file,
                               exc_info=True)

    def stop(self):
        """Implementation of the abstract method stop.
        """

        # close base class zmq sockets
        self.close_socket()


class Cleaner(CleanerBase):
    """
    Implementation of the cleaner when handling http detectors.
    """

    def remove_element(self, base_path, file_id):

        # generate file path
        source_file = os.path.join(base_path, file_id)

        # remove file
        response = requests.delete(source_file)

        try:
            response.raise_for_status()
            self.log.debug("Deleting file '%s' succeeded.", source_file)
        except Exception:
            self.log.error("Deleting file '%s' failed.", source_file,
                           exc_info=True)
