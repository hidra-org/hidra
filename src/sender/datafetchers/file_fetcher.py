from __future__ import print_function
from __future__ import unicode_literals

import os
import json
import shutil
import errno

from datafetcherbase import DataFetcherBase, DataHandlingError
from cleanerbase import CleanerBase
from hidra import generate_filepath
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, fetcher_id, context):

        DataFetcherBase.__init__(self,
                                 config,
                                 log_queue,
                                 fetcher_id,
                                 "file_fetcher-{}".format(fetcher_id),
                                 context)

        self.config = config
        self.log_queue = log_queue

        self.source_file = None
        self.target_file = None
        self.is_windows = None
        self.finish = None

        self.required_params = ["fix_subdirs", "store_data"]

        self.check_config()
        self.setup()

    def setup(self):
        """
        Sets static configuration parameters and which finish method to use.
        """

        self.config["send_timeout"] = -1  # 10
        self.config["remove_flag"] = False

        self.is_windows = utils.is_windows()

        if (self.config["remove_data"] == "with_confirmation"
                and self.config["use_data_stream"]):
            self.finish = self.finish_with_cleaner
        else:
            self.finish = self.finish_without_cleaner

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.
        """

        # Build source file
        self.source_file = generate_filepath(metadata["source_path"],
                                             metadata)
        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config["local_target"],
                                             metadata)

        if targets:
            try:
                self.log.debug("get filesize for '{}'..."
                               .format(self.source_file))
                filesize = os.path.getsize(self.source_file)
                file_mod_time = os.stat(self.source_file).st_mtime
                file_create_time = os.stat(self.source_file).st_ctime
                self.log.debug("filesize({}) = {}"
                               .format(self.source_file, filesize))
                self.log.debug("file_mod_time({}) = {}"
                               .format(self.source_file, file_mod_time))

            except:
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
                    # http://softwareengineering.stackexchange.com/questions/245156/is-it-safe-to-convert-windows-file-paths-to-unix-file-paths-with-a-simple-replac  # noqa E501
                    metadata["source_path"] = (
                        metadata["source_path"].replace("\\", "/"))
                    metadata["relative_path"] = (
                        metadata["relative_path"].replace("\\", "/"))

                metadata["filesize"] = filesize
                metadata["file_mod_time"] = file_mod_time
                metadata["file_create_time"] = file_create_time
                metadata["chunksize"] = self.config["chunksize"]
                if self.config["remove_data"] == "with_confirmation":
                    metadata["confirmation_required"] = self.confirmation_topic
                else:
                    metadata["confirmation_required"] = False

                self.log.debug("metadata = {}".format(metadata))
            except:
                self.log.error("Unable to assemble multi-part message.")
                raise

    def send_data(self, targets, metadata, open_connections):
        """Implementation of the abstract method send_data.
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
            self.log.debug("Opening '{}'...".format(self.source_file))
            file_descriptor = open(str(self.source_file), "rb")
        except:
            self.log.error("Unable to read source file '{}'"
                           .format(self.source_file), exc_info=True)
            raise

        self.log.debug("Passing multipart-message for file '{}'..."
                       .format(self.source_file))
        # sending data divided into chunks
        while True:

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
                    json.dumps(chunk_metadata).encode("utf-8"))
                chunk_payload.append(file_content)
            except:
                self.log.error("Unable to pack multipart-message for file "
                               "'{}'".format(self.source_file), exc_info=True)

            # send message to data targets
            try:
                self.send_to_targets(targets=targets_data,
                                     open_connections=open_connections,
                                     metadata=None,
                                     payload=chunk_payload)
            except DataHandlingError:
                self.log.error("Unable to send multipart-message for file "
                               "'{}' (chunk {})".format(self.source_file,
                                                        chunk_number),
                               exc_info=True)
                send_error = True
            except:
                self.log.error("Unable to send multipart-message for file "
                               "'{}' (chunk {})".format(self.source_file,
                                                        chunk_number),
                               exc_info=True)

            chunk_number += 1

        # close file
        try:
            self.log.debug("Closing '{}'...".format(self.source_file))
            file_descriptor.close()
        except:
            self.log.error("Unable to close target file '{}'"
                           .format(self.source_file), exc_info=True)
            raise

        # do not remove data until a confirmation is sent back from the
        # priority target
        if self.config["remove_data"] == "with_confirmation":
            self.config["remove_flag"] = False

        # the data was successfully sent -> mark it as removable
        elif not send_error:
            self.config["remove_flag"] = True

    def _datahandling(self, action_function, metadata):
        try:
            action_function(self.source_file, self.target_file)
        except IOError as e:

            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:
                subdir, tmp = os.path.split(metadata["relative_path"])
                target_base_path = os.path.join(
                    self.target_file.split(subdir + os.sep)[0], subdir)

                if metadata["relative_path"] in self.config["fix_subdirs"]:
                    self.log.error("Unable to copy/move file '{}' to '{}': "
                                   "Directory {} is not available"
                                   .format(self.source_file, self.target_file,
                                           metadata["relative_path"]))
                    raise
                elif (subdir in self.config["fix_subdirs"]
                        and not os.path.isdir(target_base_path)):
                    self.log.error("Unable to copy/move file '{}' to '{}': "
                                   "Directory {} is not available"
                                   .format(self.source_file,
                                           self.target_file,
                                           subdir))
                    raise
                else:
                    try:
                        target_path, filename = os.path.split(self.target_file)
                        os.makedirs(target_path)
                        self.log.info("New target directory created: {}"
                                      .format(target_path))
                        action_function(self.source_file, self.target_file)
                    except OSError as e:
                        self.log.info("Target directory creation failed, was "
                                      "already created in the meantime: {}"
                                      .format(target_path))
                        action_function(self.source_file, self.target_file)
                    except:
                        err_msg = ("Unable to copy/move file '{}' to '{}'"
                                   .format(self.source_file, self.target_file))
                        self.log.error(err_msg, exc_info=True)
                        self.log.debug("target_path: {}".format(target_path))
                        raise
            else:
                self.log.error("Unable to copy/move file '{}' to '{}'"
                               .format(self.source_file, self.target_file),
                               exc_info=True)
                raise
        except:
            self.log.error("Unable to copy/move file '{}' to '{}'"
                           .format(self.source_file, self.target_file),
                           exc_info=True)
            raise

    def finish(self, targets, metadata, open_connections):
        """Implementation of the abstract method finish.

        Is overwritten when class is instantiated depending if a cleaner class
        is used or not
        """
        pass

    def finish_with_cleaner(self, targets, metadata, open_connections):
        """Finish method to be used if use of cleaner was configured.
        """

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

        # copy file
        # (does not preserve file owner, group or ACLs)
        if self.config["store_data"]:
            try:
                self._datahandling(shutil.copy, metadata)
                self.log.info("Copying file '{}' ...success."
                              .format(self.source_file))
            except:
                return

        # remove file
        elif self.config["remove_data"]:

            file_id = self.generate_file_id(metadata)
            try:
                # round up the division result
                n_chunks = -(-metadata["filesize"] // metadata["chunksize"])
            except KeyError:
                filesize = os.path.getsize(self.source_file)
                # round up the division result
                n_chunks = -(-filesize // self.config["chunksize"])

            self.cleaner_job_socket.send_multipart(
                [metadata["source_path"].encode("utf-8"),
                 file_id.encode("utf-8"),
                 str(n_chunks)]
            )
            self.log.debug("Forwarded to cleaner {}".format(file_id))

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(targets=targets_metadata,
                                     open_connections=open_connections,
                                     metadata=metadata,
                                     payload=None,
                                     timeout=self.config["send_timeout"])
                self.log.debug("Passing metadata multipart-message for file "
                               "{}...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{}' to '{}'"
                               .format(self.source_file, targets_metadata),
                               exc_info=True)

    def finish_without_cleaner(self, targets, metadata, open_connections):
        """Finish method to use when use of cleaner not configured.
        """

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

        # move file
        if (self.config["store_data"]
                and self.config["remove_data"]
                and self.config["remove_flag"]):

            try:
                self._datahandling(shutil.move, metadata)
                self.log.info("Moving file '{}' to '{}'...success."
                              .format(self.source_file, self.target_file))
            except:
                self.log.error("Could not move file {} to {}"
                               .format(self.source_file, self.target_file),
                               exc_info=True)
                return

        # copy file
        # (does not preserve file owner, group or ACLs)
        elif self.config["store_data"]:
            try:
                self._datahandling(shutil.copy, metadata)
                self.log.info("Copying file '{}' ...success."
                              .format(self.source_file))
            except:
                return

        # remove file
        elif self.config["remove_data"] and self.config["remove_flag"]:
            try:
                os.remove(self.source_file)
                self.log.info("Removing file '{}' ...success."
                              .format(self.source_file))
            except:
                self.log.error("Unable to remove file {}"
                               .format(self.source_file), exc_info=True)

            self.config["remove_flag"] = False

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(targets=targets_metadata,
                                     open_connections=open_connections,
                                     metadata=metadata,
                                     payload=None,
                                     timeout=self.config["send_timeout"])
                self.log.debug("Passing metadata multipart-message for file "
                               "{}...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{}' to '{}'"
                               .format(self.source_file, targets_metadata),
                               exc_info=True)

    def stop(self):
        """Implementation of the abstract method stop.
        """
        # cloes base class zmq sockets
        self.close_socket()


class Cleaner(CleanerBase):
    def remove_element(self, base_path, file_id):

        # generate file path
        source_file = os.path.join(base_path, file_id)

        # remove file
        try:
            os.remove(source_file)
            self.log.info("Removing file '{}' ...success".format(source_file))
        except:
            self.log.error("Unable to remove file {}".format(source_file),
                           exc_info=True)
