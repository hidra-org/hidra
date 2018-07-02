from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import abc
import json
import os
import sys
import zmq

import __init__ as init  # noqa F401
from base_class import Base
import utils
from utils import WrongConfiguration

# source:
# http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5  # noqa E501
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataHandlingError(Exception):
    pass


class DataFetcherBase(Base, ABC):

    def __init__(self, config, log_queue, fetcher_id, logger_name, context):
        """Initial setup

        Checks if the required parameters are set in the configuration for
        the base setup.

        Args:
            config (dict): A dictionary containing the configuration
                           parameters.
            log_queue: The multiprocessing queue which is used for logging.
            fetcher_id (int): The ID of this datafetcher instance.
            logger_name (str): The name to be used for the logger.
            context: The ZMQ context to be used.

        """

        self.fetcher_id = fetcher_id
        self.config = config
        self.context = context
        self.cleaner_job_socket = None

        self.log = utils.get_logger(logger_name, log_queue)

        self.source_file = None
        self.target_file = None

        self.required_params = []

        self.required_base_params = [
            "chunksize",
            "local_target",
            ["remove_data", [True,
                             False,
                             "stop_on_error",
                             "with_confirmation"]],
            "endpoints",
            "main_pid"
        ]

        self.check_config(print_log=False)
        self.base_setup()

    def check_config(self, print_log=True):
        """Check that the configuration containes the nessessary parameters.

        Args:
            print_log (boolean): If a summary of the configured parameters
                                 should be logged.
        Raises:
            WrongConfiguration: The configuration has missing or
                                wrong parameteres.
        """

        # combine paramerters which aare needed for all datafetchers with the
        # specific ones for this fetcher
        required_params = self.required_base_params + self.required_params

        # Check format of config
        check_passed, config_reduced = utils.check_config(
            required_params,
            self.config,
            self.log
        )

        if check_passed:
            if print_log:
                formated_config = str(
                    json.dumps(config_reduced, sort_keys=True, indent=4)
                )
                self.log.info("Configuration for data fetcher: {}"
                              .format(formated_config))

        else:
            # self.log.debug("config={}".format(self.config))
            msg = "The configuration has missing or wrong parameteres."
            raise WrongConfiguration(msg)

    def base_setup(self):
        """Sets up the shared components needed by all datafetchers.

        Created a socket to communicate with the cleaner and sets the topic.
        """

        if self.config["remove_data"] == "with_confirmation":
            # create socket
            self.cleaner_job_socket = self.start_socket(
                name="cleaner_job_socket",
                sock_type=zmq.PUSH,
                sock_con="connect",
                endpoint=self.config["endpoints"].cleaner_job_con
            )

            self.confirmation_topic = (
                utils.generate_sender_id(self.config["main_pid"])
            )

    def send_to_targets(self,
                        targets,
                        open_connections,
                        metadata,
                        payload,
                        timeout=-1):
        """Send the data to targets.

        Args:
            targets: A list of targets where to send the data to.
                     Each taget has is of the form:
                        - target: A ZMQ endpoint to send the data to.
                        - prio (int): With which priority this data should be
                                      sent:
                                      - 0 is highes priority with blocking
                                      - all other prioirities are nonblocking
                                        but sorted numerically.
                        - send_type: If the data (means payload and metadata)
                                     or only the metadata should be sent.

            open_connections (dict): Containing all open sockets. If data was
                                     send to a target already the socket is
                                     kept open till the target disconnects.
            metadata: The metadata of this data block.
            payload: The data block to be sent.
            timeout (optional): How long to wait for the message to be received
                                (default: -1, means wait forever)
        """

        for target, prio, send_type in targets:

            # send data to the data stream to store it in the storage system
            if prio == 0:
                # socket not known
                if target not in open_connections:
                    endpoint = "tcp://{}".format(target)
                    # open socket
                    try:
                        # start and register socket
                        open_connections[target] = self.start_socket(
                            name="socket",
                            sock_type=zmq.PUSH,
                            sock_con="connect",
                            endpoint=endpoint
                        )
                    except:
                        self.log.debug("Raising DataHandling error",
                                       exc_info=True)
                        msg = ("Failed to start socket (connect): '{}'"
                               .format(endpoint))
                        raise DataHandlingError(msg)

                # send data
                try:
                    if send_type == "data":
                        tracker = open_connections[target].send_multipart(
                            payload,
                            copy=False,
                            track=True
                        )
                        self.log.info("Sending message part from file '{}' "
                                      "to '{}' with priority {}"
                                      .format(self.source_file, target, prio))

                    elif send_type == "metadata":
                        # json.dumps(None) is 'N.'
                        tracker = open_connections[target].send_multipart(
                            [json.dumps(metadata).encode("utf-8"),
                             json.dumps(None).encode("utf-8")],
                            copy=False,
                            track=True
                        )
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
                    self.log.debug("Raising DataHandling error", exc_info=True)
                    msg = ("Sending (metadata of) message part from file '{}' "
                           "to '{}' with priority {} failed."
                           .format(self.source_file, target, prio))
                    raise DataHandlingError(msg)

            else:
                # socket not known
                if target not in open_connections:
                    # start and register socket
                    open_connections[target] = self.start_socket(
                        name="socket",
                        sock_type=zmq.PUSH,
                        sock_con="connect",
                        endpoint="tcp://{}".format(target)
                    )
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
                        zmq.NOBLOCK
                    )
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
        self.stop_socket("cleaner_job_socket")

    @abc.abstractmethod
    def stop(self):
        """Stop and clean up.
        """
        pass

    def __exit__(self, type, value, traceback):
        self.close_socket()
        self.stop()

    def __del__(self):
        self.close_socket()
        self.stop()
