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
This module implements a data fetcher for handling files.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import errno
import json
import os
import shutil
import subprocess
import time

try:
    from pathlib2 import Path
except ImportError:
    from pathlib import Path

from datafetcherbase import DataFetcherBase, DataHandlingError
from cleanerbase import CleanerBase
from hidra import generate_filepath
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

# for platform-independency
# WindowsError only exists on Windows machines
if not getattr(__builtins__, "WindowsError", None):
    class WindowsError(OSError):
        """ Define WindowsError on Unix systems """
        pass


class DataFetcher(DataFetcherBase):
    """
    Implementation of the data fetcher to handle files.
    """

    def __init__(self, config, log_queue, fetcher_id, context, lock):

        DataFetcherBase.__init__(self,
                                 config,
                                 log_queue,
                                 fetcher_id,
                                 "file_fetcher-{}".format(fetcher_id),
                                 context,
                                 lock)

        # base class sets
        #   self.config_all - all configurations
        #   self.config_df - the config of the datafetcher
        #   self.config - the module specific config
        #   self.df_type -  the name of the datafetcher module
        #   self.log_queue
        #   self.log

        self.source_file = None
        self.target_file = None
        self.is_windows = None
        self.finish = None

        self.windows_handle_path = None

        self.keep_running = True

        self.required_params = ["fix_subdirs"]

        # check that the required_params are set inside of module specific
        # config
        self.check_config()

        self._setup()

    def _setup(self):
        """
        Sets static configuration parameters and which finish method to use.
        """

        self.config["send_timeout"] = -1  # 10
        self.config["remove_flag"] = False

        try:
            self.windows_handle_path = self.config["windows_handle_path"]
        except KeyError:
            pass

        self.is_windows = utils.is_windows()

        if (self.config_df["remove_data"] == "with_confirmation"
                and self.config_df["use_data_stream"]):
            self.log.debug("Set finish to finish_with_cleaner")
            self.finish = self.finish_with_cleaner
        else:
            self.finish = self.finish_without_cleaner

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata to extend.
        """

        # Build source file
        self.source_file = generate_filepath(metadata["source_path"],
                                             metadata)
        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config_df["local_target"],
                                             metadata)

        if targets:
            try:
                self.log.debug("get filesize for '%s'...", self.source_file)
                filesize = os.path.getsize(self.source_file)
                file_mod_time = os.stat(self.source_file).st_mtime
                file_create_time = os.stat(self.source_file).st_ctime
                self.log.debug("filesize(%s) = %s", self.source_file, filesize)
                self.log.debug("file_mod_time(%s) = %s", self.source_file,
                               file_mod_time)

            except Exception:
                self.log.error("Unable to create metadata dictionary.")
                raise

            try:
                self.log.debug("create metadata for source file...")
                # metadata = {
                #        "filename"       : ...,
                #        "source_path"     : ...,  # in unix format
                #        "relative_path"   : ...,  # in unix format
                #        "filesize"       : ...,
                #        "file_mod_time"    : ...,
                #        "file_create_time" : ...,
                #        "chunksize"      : ...
                #        }
                if self.is_windows:
                    # path convertions is save, see:
                    # pylint: disable=line-too-long
                    # http://softwareengineering.stackexchange.com/questions/245156/is-it-safe-to-convert-windows-file-paths-to-unix-file-paths-with-a-simple-replac  # noqa E501
                    metadata["source_path"] = (
                        metadata["source_path"].replace("\\", "/"))
                    metadata["relative_path"] = (
                        metadata["relative_path"].replace("\\", "/"))

                metadata["filesize"] = filesize
                metadata["file_mod_time"] = file_mod_time
                metadata["file_create_time"] = file_create_time
                metadata["chunksize"] = self.config_df["chunksize"]
                if self.config_df["remove_data"] == "with_confirmation":
                    metadata["confirmation_required"] = (
                        self.confirmation_topic.decode()
                    )
                else:
                    metadata["confirmation_required"] = False

                self.log.debug("metadata = %s", metadata)
            except Exception:
                self.log.error("Unable to assemble multi-part message.")
                raise

    def send_data(self, targets, metadata, open_connections):
        """Implementation of the abstract method send_data.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metadata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
        """

        # no targets to send data to -> data can be removed
        # (after possible local storing)
        if not targets:
            self.config["remove_flag"] = True
            return

        # find the targets requesting for data
        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_data = [i for i in targets if i[2] == "data"]

        # no targets to send data to
        if not targets_data:
            self.config["remove_flag"] = True
            return

        self.config["remove_flag"] = False
        chunksize = metadata["chunksize"]

        chunk_number = 0
        send_error = False

        # reading source file into memory
        try:
            self.log.debug("Opening '%s'...", self.source_file)
            file_descriptor = open(str(self.source_file), "rb")
        except Exception:
            self.log.error("Unable to read source file '%s'",
                           self.source_file, exc_info=True)
            raise

        self.log.debug("Passing multipart-message for file '%s'...",
                       self.source_file)
        # sending data divided into chunks
        while self.keep_running:

            # read next chunk from file
            file_content = file_descriptor.read(chunksize)

            # detect if end of file has been reached
            if not file_content:
                if chunk_number == 0:
                    self.log.debug("File is empty. Skip sending to target.")
                break

            try:
                # assemble metadata for zmq-message
                chunk_metadata = metadata.copy()
                chunk_metadata["chunk_number"] = chunk_number

                chunk_payload = []
                chunk_payload.append(
                    json.dumps(chunk_metadata).encode("utf-8")
                )
                chunk_payload.append(file_content)
            except Exception:
                self.log.error("Unable to pack multipart-message for file "
                               "'%s'", self.source_file, exc_info=True)

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
                send_error = True
            except Exception:
                self.log.error("Unable to send multipart-message for file "
                               "'%s' (chunk %s)", self.source_file,
                               chunk_number, exc_info=True)

            chunk_number += 1

        # close file
        try:
            self.log.debug("Closing '%s'...", self.source_file)
            file_descriptor.close()
        except Exception:
            self.log.error("Unable to close target file '%s'",
                           self.source_file, exc_info=True)
            raise

        # do not remove data until a confirmation is sent back from the
        # priority target
#        if self.config_df["remove_data"] == "with_confirmation":
#            self.config["remove_flag"] = False
#
#        # the data was successfully sent -> mark it as removable
#        elif not send_error:
        if not send_error:
            self.config["remove_flag"] = True

    def _datahandling(self, action_function, metadata):
        try:
            action_function(self.source_file, self.target_file)
        except IOError as excp:

            # errno.ENOENT == "No such file or directory"
            if excp.errno == errno.ENOENT:
                subdir, _ = os.path.split(metadata["relative_path"])
                target_base_path = os.path.join(
                    self.target_file.split(subdir + os.sep)[0], subdir
                )

                if metadata["relative_path"] in self.config["fix_subdirs"]:
                    self.log.error("Unable to copy/move file '%s' to '%s': "
                                   "Directory %s is not available",
                                   self.source_file, self.target_file,
                                   metadata["relative_path"])
                    raise
                elif (subdir in self.config["fix_subdirs"]
                      and not os.path.isdir(target_base_path)):
                    self.log.error("Unable to copy/move file '%s' to '%s': "
                                   "Directory %s is not available",
                                   self.source_file, self.target_file,
                                   subdir)
                    raise
                else:
                    try:
                        target_path, _ = os.path.split(self.target_file)
                        os.makedirs(target_path)
                        self.log.info("New target directory created: %s",
                                      target_path)
                        action_function(self.source_file, self.target_file)
                    except OSError:
                        self.log.info("Target directory creation failed, was "
                                      "already created in the meantime: %s",
                                      target_path)
                        action_function(self.source_file, self.target_file)
                    except:
                        err_msg = ("Unable to copy/move file '%s' to '%s'",
                                   self.source_file, self.target_file)
                        self.log.error(err_msg, exc_info=True)
                        self.log.debug("target_path: %s", target_path)
                        raise
            else:
                self.log.error("Unable to copy/move file '%s' to '%s'",
                               self.source_file, self.target_file,
                               exc_info=True)
                raise
        except Exception:
            self.log.error("Unable to copy/move file '%s' to '%s'",
                           self.source_file, self.target_file,
                           exc_info=True)
            raise

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

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

        # copy file
        # (does not preserve file owner, group or ACLs)
        if self.config_df["store_data"]:
            try:
                self._datahandling(shutil.copy, metadata)
                self.log.info("Copying file '%s' ...success.",
                              self.source_file)
            except Exception:
                self.log.error("Could not copy file %s to %s",
                               self.source_file, self.target_file,
                               exc_info=True)
                return

        # remove file
        # can be set/done in addition to copy -> no elif
        # (e.g. store_data = True and remove_data = with_confirmation)
        if self.config_df["remove_data"] and self.config["remove_flag"]:

            file_id = self.generate_file_id(metadata)
            try:
                # round up the division result
                n_chunks = -(-metadata["filesize"] // metadata["chunksize"])
            except KeyError:
                filesize = os.path.getsize(self.source_file)
                # round up the division result
                n_chunks = -(-filesize // self.config_df["chunksize"])

            self.cleaner_job_socket.send_multipart(
                [metadata["source_path"].encode("utf-8"),
                 file_id.encode("utf-8"),
                 str(n_chunks).encode("utf-8")]
            )
            self.log.debug("Forwarded to cleaner %s", file_id)

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(targets=targets_metadata,
                                     open_connections=open_connections,
                                     metadata=metadata,
                                     payload=None,
                                     chunk_number=None,
                                     timeout=self.config["send_timeout"])
                self.log.debug("Passing metadata multipart-message for file "
                               "%s...done.", self.source_file)

            except Exception:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '%s' to '%s'", self.source_file,
                               targets_metadata, exc_info=True)

    def finish_without_cleaner(self, targets, metadata, open_connections):
        """Finish method to use when use of cleaner not configured.

        Args:
            targets (list): The target list this file is supposed to go.
            metadata (dict): The dictionary with the metedata of the file
            open_connections (dict): The dictionary containing all open zmq
                                     connections.
        """

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

        # move file
        if (self.config_df["store_data"]
                and self.config_df["remove_data"]
                and self.config["remove_flag"]):

            try:
                self._datahandling(shutil.move, metadata)
                self.log.info("Moving file '%s' to '%s'...success.",
                              self.source_file, self.target_file)
            except Exception:
                self.log.error("Could not move file %s to %s",
                               self.source_file, self.target_file,
                               exc_info=True)
                return

        # copy file
        # (does not preserve file owner, group or ACLs)
        elif self.config_df["store_data"]:
            try:
                self._datahandling(shutil.copy, metadata)
                self.log.info("Copying file '%s' ...success.",
                              self.source_file)
            except Exception:
                self.log.error("Could not copy file %s to %s",
                               self.source_file, self.target_file,
                               exc_info=True)
                return

        # remove file
        elif self.config_df["remove_data"] and self.config["remove_flag"]:
            try:
                os.remove(self.source_file)
                self.log.info("Removing file '%s' ...success.",
                              self.source_file)
            except OSError as err:
                if type(err).__name__ == "WindowsError":
                    self.log.error("Windows Error occured.")

                    self._get_file_handle_info()
                    self._retry_remove()
                else:
                    self.log.error("Unable to remove file %s",
                                   self.source_file, exc_info=True)

            except Exception:
                self.log.error("Unable to remove file %s", self.source_file,
                               exc_info=True)
            finally:
                self.config["remove_flag"] = False

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(targets=targets_metadata,
                                     open_connections=open_connections,
                                     metadata=metadata,
                                     payload=None,
                                     chunk_number=None,
                                     timeout=self.config["send_timeout"])
                self.log.debug("Passing metadata multipart-message for file "
                               "%s...done.", self.source_file)

            except Exception:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '%s' to '%s'", self.source_file,
                               targets_metadata, exc_info=True)

    def _get_file_handle_info(self):

        if self.windows_handle_path is None:
            self.log.info("No windows handle path specified.")
            return

        self.log.debug("Check open file handles for %s", self.source_file)

        try:
            self.log.debug("current pid: %s", os.getpid())

            # source file is an absolute path which windows handle cannot find
            file_to_check = os.path.basename(self.source_file)
            self.log.debug("checking for %s", file_to_check)

            self.log.debug("current processes accessing the file: %s",
                           subprocess.check_output([self.windows_handle_path,
                                                    file_to_check]))

        except Exception:
            self.log.error("Collecting debug information failed.",
                           exc_info=True)

    def _retry_remove(self):

        self.log.debug("Try to wait till the system lock disappears and try "
                       "again.")

        n_iter = 5
        for i in range(n_iter):
            time.sleep(0.2)
            try:
                os.remove(self.source_file)
                self.log.info("Removing file '%s' ...success (%s/%s).",
                              self.source_file, i, n_iter)
                break
            except Exception:
                self.log.error("Unable to remove file %s (%s/%s)",
                               self.source_file, i, n_iter, exc_info=True)

    def stop(self):
        """Implementation of the abstract method stop.
        """

        self.keep_running = False

        # stop everything started in the base class
        self.stop_base()

        # cloes base class zmq sockets
        self.close_socket()


class Cleaner(CleanerBase):
    """
    Implementation of the cleaner when handling files.
    """

    def remove_element(self, base_path, file_id):

        # generate file path
        source_file = Path(os.path.join(base_path, file_id)).as_posix()

        # remove file
        try:
            os.remove(source_file)
            self.log.info("Removing file '%s' ...success", source_file)
        except Exception:
            self.log.error("Unable to remove file %s", source_file,
                           exc_info=True)
