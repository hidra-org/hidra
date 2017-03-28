from __future__ import unicode_literals

import zmq
import json
import sys
import abc

# source: http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

from __init__ import BASE_PATH
import helpers


__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataHandlingError(Exception):
    pass


class DataFetcherBase(ABC):
    def __init__(self, config, log_queue, id, logger_name):
        """Initial setup

        Checks if all required parameters are set in the configuration
        """

        self.id = id
        self.config = config

        self.log = helpers.get_logger(logger_name, log_queue)

        self.source_file = None
        self.target_file = None

        required_params = ["chunksize",
                           "local_target"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            self.config,
                                                            self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {0}"
                          .format(config_reduced))
        else:
            self.log.debug("config={0}".format(self.config))
            raise Exception("Wrong configuration")

    def send_to_targets(self, targets, open_connections, metadata, payload,
                        context, timeout=-1):

        for target, prio, suffixes, send_type in targets:

            # send data to the data stream to store it in the storage system
            if prio == 0:
                # socket not known
                if target not in open_connections:
                    # open socket
                    try:
                        socket = context.socket(zmq.PUSH)
                        connection_str = "tcp://{0}".format(target)

                        socket.connect(connection_str)
                        self.log.info("Start socket (connect): '{0}'"
                                      .format(connection_str))

                        # register socket
                        open_connections[target] = socket
                    except:
                        raise DataHandlingError("Failed to start socket "
                                                "(connect): '{0}'"
                                                .format(connection_str))

                # send data
                try:
                    if send_type == "data":
                        tracker = open_connections[target].send_multipart(
                            payload, copy=False, track=True)
                        self.log.info("Sending message part from file '{0}' "
                                      "to '{1}' with priority {2}"
                                      .format(self.source_file, target, prio))

                    elif send_type == "metadata":
                        # json.dumps(None) is 'N.'
                        tracker = open_connections[target].send_multipart(
                            [json.dumps(metadata).encode("utf-8"),
                             json.dumps(None).encode("utf-8")],
                            copy=False, track=True)
                        self.log.info("Sending metadata of message part from "
                                      "file '{0}' to '{1}' with priority {2}"
                                      .format(self.source_file, target, prio))
                        self.log.debug("metadata={0}".format(metadata))

                    if not tracker.done:
                        self.log.debug("Message part from file '{0}' has not "
                                       "been sent yet, waiting..."
                                       .format(self.source_file))
                        tracker.wait(timeout)
                        self.log.debug("Message part from file '{0}' has not "
                                       "been sent yet, waiting...done"
                                       .format(self.source_file))

                except:
                    raise DataHandlingError("Sending (metadata of) message "
                                            "part from file '{0}' to '{1}' "
                                            "with priority {2} failed."
                                            .format(self.source_file, target,
                                                    prio))

            else:
                # socket not known
                if target not in open_connections:
                    # open socket
                    socket = context.socket(zmq.PUSH)
                    connection_str = "tcp://{0}".format(target)

                    socket.connect(connection_str)
                    self.log.info("Start socket (connect): '{0}'"
                                  .format(connection_str))

                    # register socket
                    open_connections[target] = socket

                # send data
                if send_type == "data":
                    open_connections[target].send_multipart(payload,
                                                            zmq.NOBLOCK)
                    self.log.info("Sending message part from file '{0}' to "
                                  "'{1}' with priority {2}"
                                  .format(self.source_file, target, prio))

                elif send_type == "metadata":
                    open_connections[target].send_multipart(
                        [json.dumps(metadata).encode("utf-8"),
                         json.dumps(None).encode("utf-8")],
                        zmq.NOBLOCK)
                    self.log.info("Sending metadata of message part from file "
                                  "'{0}' to '{1}' with priority {2}"
                                  .format(self.source_file, target, prio))
                    self.log.debug("metadata={0}".format(metadata))

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
                    [[<node_name>:<port>, <priority>,
                      <list of file suffixes>, <request_type>], ...]
                where
                    <list of file suffixes>: is a python of file types which
                                             should be send to this target.
                                             e.g. [u'.cbf']
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
    def send_data(self, targets, metadata, open_connections, context):
        """Reads data into buffer and sends it to all targets

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>,
                      <list of file suffixes>, <request_type>], ...]
                where
                    <list of file suffixes>: is a python of file types which
                                             should be send to this target.
                                             e.g. [u'.cbf']
                    <request_type>: u'data' or u'metadata'
            metadata (dict): extendet metadata dictionary filled by function
                             get_metadata
            open_connections (dict)
            context: zmq context

        Returns:
            Nothing
        """
        pass

    @abc.abstractmethod
    def finish(self, targets, metadata, open_connections, context):
        """

        Args:
            targets (list):
                A list of targets where the data or metadata should be sent to.
                It is of the form:
                    [[<node_name>:<port>, <priority>,
                      <list of file suffixes>, <request_type>], ...]
                where
                    <list of file suffixes>: is a python of file types which
                                             should be send to this target.
                                             e.g. [u'.cbf']
                    <request_type>: u'data' or u'metadata'
            metadata (dict)
            open_connections
            context

        Returns:
            Nothing
        """
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()