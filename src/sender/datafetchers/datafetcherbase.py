from __future__ import unicode_literals

import abc
import json
import os
import sys
import zmq

from __init__ import BASE_PATH  # noqa F401
import utils

# source:
# http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5  # noqa E501
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataHandlingError(Exception):
    pass


class DataFetcherBase(ABC):

    def __init__(self, config, log_queue, id, logger_name, context):
        """Initial setup

        Checks if all required parameters are set in the configuration
        """

        self.id = id
        self.config = config
        self.context = context

        self.log = utils.get_logger(logger_name, log_queue)

        self.source_file = None
        self.target_file = None

        required_params = [
            "chunksize",
            "local_target",
            ["remove_data", [True,
                             False,
                             "stop_on_error",
                             "with_confirmation"]],
            "cleaner_job_con_str",
            "main_pid"
        ]

        # Check format of config
        check_passed, config_reduced = utils.check_config(required_params,
                                                          self.config,
                                                          self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {}"
                          .format(config_reduced))

            if self.config["remove_data"] == "with_confirmation":
                # create socket
                try:
                    self.cleaner_job_socket = self.context.socket(zmq.PUSH)
                    self.cleaner_job_socket.connect(
                        self.config["cleaner_job_con_str"])
                    self.log.info("Start cleaner job_socket (connect): {0}"
                                  .format(self.config["cleaner_job_con_str"]))
                except:
                    self.log.error("Failed to start cleaner job socket "
                                   "(connect): '{0}'".format(
                                       self.config["cleaner_job_con_str"]),
                                   exc_info=True)
                    raise

                self.confirmation_topic = (
                    utils.generate_sender_id(self.config["main_pid"])
                )
            else:
                self.cleaner_job_socket = None

        else:
            # self.log.debug("config={0}".format(self.config))
            raise Exception("Wrong configuration")

    def send_to_targets(self, targets, open_connections, metadata, payload,
                        timeout=-1):

        for target, prio, send_type in targets:

            # send data to the data stream to store it in the storage system
            if prio == 0:
                # socket not known
                if target not in open_connections:
                    # open socket
                    try:
                        socket = self.context.socket(zmq.PUSH)
                        connection_str = "tcp://{}".format(target)

                        socket.connect(connection_str)
                        self.log.info("Start socket (connect): '{}'"
                                      .format(connection_str))

                        # register socket
                        open_connections[target] = socket
                    except:
                        raise DataHandlingError("Failed to start socket "
                                                "(connect): '{}'"
                                                .format(connection_str))

                # send data
                try:
                    if send_type == "data":
                        tracker = open_connections[target].send_multipart(
                            payload, copy=False, track=True)
                        self.log.info("Sending message part from file '{}' "
                                      "to '{}' with priority {}"
                                      .format(self.source_file, target, prio))

                    elif send_type == "metadata":
                        # json.dumps(None) is 'N.'
                        tracker = open_connections[target].send_multipart(
                            [json.dumps(metadata).encode("utf-8"),
                             json.dumps(None).encode("utf-8")],
                            copy=False, track=True)
                        self.log.info("Sending metadata of message part from "
                                      "file '{}' to '{}' with priority {}"
                                      .format(self.source_file, target, prio))
                        self.log.debug("metadata={}".format(metadata))

                    if not tracker.done:
                        self.log.debug("Message part from file '{}' has not "
                                       "been sent yet, waiting..."
                                       .format(self.source_file))
                        tracker.wait(timeout)
                        self.log.debug("Message part from file '{}' has not "
                                       "been sent yet, waiting...done"
                                       .format(self.source_file))

                except:
                    raise DataHandlingError("Sending (metadata of) message "
                                            "part from file '{}' to '{}' "
                                            "with priority {} failed."
                                            .format(self.source_file, target,
                                                    prio))

            else:
                # socket not known
                if target not in open_connections:
                    # open socket
                    socket = self.context.socket(zmq.PUSH)
                    connection_str = "tcp://{}".format(target)

                    socket.connect(connection_str)
                    self.log.info("Start socket (connect): '{}'"
                                  .format(connection_str))

                    # register socket
                    open_connections[target] = socket

                # send data
                if send_type == "data":
                    open_connections[target].send_multipart(payload,
                                                            zmq.NOBLOCK)
                    self.log.info("Sending message part from file '{}' to "
                                  "'{}' with priority {}"
                                  .format(self.source_file, target, prio))

                elif send_type == "metadata":
                    open_connections[target].send_multipart(
                        [json.dumps(metadata).encode("utf-8"),
                         json.dumps(None).encode("utf-8")],
                        zmq.NOBLOCK)
                    self.log.info("Sending metadata of message part from file "
                                  "'{}' to '{}' with priority {}"
                                  .format(self.source_file, target, prio))
                    self.log.debug("metadata={}".format(metadata))

    def generate_file_id(self, metadata):
        """Generates a file id consisting of relative path and file name
        """
        # generate file identifier
        if (metadata["relative_path"] == ""
                or metadata["relative_path"] is None):
            file_id = metadata["filename"]
        # if the relative path starts with a slash path.join will consider it
        # as absolute path
        elif metadata["relative_path"].startswith("/"):
            file_id = os.path.join(metadata["relative_path"][1:],
                                   metadata["filename"])
        else:
            file_id = os.path.join(metadata["relative_path"],
                                   metadata["filename"])
        return file_id

    @abc.abstractmethod
    def get_metadata(self, targets, metadata):
        """Extends the given metadata and generates paths

        Reads the metadata dictionary from the event detector and extends it
        with the mandatory entries:
            - filesize (int):  the total size of the logical file (before
                               chunking)
            - file_mod_time (float, epoch time): modification time of the
                                                 logical file in epoch
            - file_create_time (float, epoch time): creation time of the
                                                    logical file in epoch
            - chunksize (int): the size of the chunks the logical message was
                               split into

        Additionally it generates the absolute source path and if a local
        target is specified the absolute target path as well.

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>, <request_type>], ...]
                where
                    <request_type>: u'data' or u'metadata'
            metadata (dict): Dictionary created by the event detector
                             containing:
                                - filename
                                - source_path
                                - relative_path

        Sets:
            source_file (str): the absolute path of the source file
            target_file (str): the absolute path for the target file

        """
        pass

    @abc.abstractmethod
    def send_data(self, targets, metadata, open_connections):
        """Reads data into buffer and sends it to all targets

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>, <request_type>], ...]
                where
                    <request_type>: u'data' or u'metadata'
            metadata (dict): extendet metadata dictionary filled by function
                             get_metadata
            open_connections (dict)

        Returns:
            Nothing
        """
        pass

    @abc.abstractmethod
    def finish(self, targets, metadata, open_connections):
        """

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>, <request_type>], ...]
                where
                    <request_type>: u'data' or u'metadata'
            metadata (dict)
            open_connections

        Returns:
            Nothing
        """
        pass

    def close_socket(self):
        if self.cleaner_job_socket is not None:
            self.cleaner_job_socket.close(0)
            self.cleaner_job_socket = None

    @abc.abstractmethod
    def stop(self):
        pass

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
