from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import abc
import json
import os
import sys
import time
import zmq

try:
    from pathlib2 import Path
except ImportError:
    # only avaliable for Python3
    from pathlib import Path

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

    def __init__(self,
                 config,
                 log_queue,
                 fetcher_id,
                 logger_name,
                 context,
                 lock):
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
            lock: A threading lock object to handle control signal access.
        """

        self.fetcher_id = fetcher_id
        self.config = config
        self.context = context
        self.lock = lock
        self.cleaner_job_socket = None

        self.log = utils.get_logger(logger_name, log_queue)

        self.source_file = None
        self.target_file = None

        self.control_signal = None
        self.keep_running = True

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

    def check_config(self, print_log=False):
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
                self.log.info("Configuration for data fetcher: {}"
                              .format(config_reduced))

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
                        chunk_number,
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
            chunk_number: The chunk number of the payload to be processed.
            timeout (optional): How long to wait for the message to be received
                                in s (default: -1, means wait forever)
        """
        timeout = 1
        self._check_control_signal()

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
                    # retry sending if anything goes wrong
                    retry_sending = True
                    while retry_sending:
                        retry_sending = False

                        if send_type == "data":
                            tracker = open_connections[target].send_multipart(
                                payload,
                                copy=False,
                                track=True
                            )
                            self.log.info("Sending message part %s from file "
                                          "'%s' to '%s' with priority %s",
                                          chunk_number, self.source_file,
                                          target, prio)

                        elif send_type == "metadata":
                            # json.dumps(None) is 'N.'
                            tracker = open_connections[target].send_multipart(
                                [json.dumps(metadata).encode("utf-8"),
                                 json.dumps(None).encode("utf-8")],
                                copy=False,
                                track=True
                            )
                            self.log.info("Sending metadata of message part "
                                          "%s from file '%s' to '%s' with "
                                          "priority %s", chunk_number,
                                          self.source_file, target, prio)
                            self.log.debug("metadata={}".format(metadata))

                        if not tracker.done:
                            self.log.debug("Message part %s from file '%s' "
                                           "has not been sent yet, waiting...",
                                           chunk_number, self.source_file)

                            while not tracker.done and self.keep_running:
                                try:
                                    tracker.wait(timeout)
                                except zmq.error.NotDone:
                                    pass

                                # check for control signals set from outside
                                if self._check_control_signal():
                                    self.log.info("Retry sending message part "
                                                  "%s from file '%s'.",
                                                  chunk_number,
                                                  self.source_file)
                                    retry_sending = True
                                    break

                            if not retry_sending:
                                self.log.debug("Message part %s from file '%s'"
                                               " has not been sent yet, "
                                               "waiting...done",
                                               chunk_number, self.source_file)

                except:
                    self.log.debug("Raising DataHandling error", exc_info=True)
                    msg = ("Sending (metadata of) message part %s from file "
                           "'%s' to '%s' with priority %s failed.",
                           chunk_number, self.source_file, target, prio)
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
                    self.log.info("Sending message part %s from file '%s' to "
                                  "'%s' with priority %s", chunk_number,
                                  self.source_file, target, prio)

                elif send_type == "metadata":
                    open_connections[target].send_multipart(
                        [json.dumps(metadata).encode("utf-8"),
                         json.dumps(None).encode("utf-8")],
                        zmq.NOBLOCK
                    )
                    self.log.info("Sending metadata of message part %s from "
                                  "file '%s' to '%s' with priority %s",
                                  chunk_number, self.source_file, target, prio)
                    self.log.debug("metadata=%s", metadata)

    def _check_control_signal(self):
        """Check for control signal and react accordingly.
        """

        woke_up = False

        if self.control_signal is None:
            return

        if  self.control_signal[0] == b"EXIT":
            self.log.debug("Received %s signal.",  self.control_signal[0])
            self.keep_running = False

        elif  self.control_signal[0] == b"CLOSE_SOCKETS":
            # do nothing
            pass

        elif  self.control_signal[0] == b"SLEEP":
            self.log.debug("Received %s signal",  self.control_signal[0])
            self._react_to_sleep_signal()
            # TODO reschedule file (part?)
            woke_up = True

        elif  self.control_signal[0] == b"WAKEUP":
            self.log.debug("Received %s signal without sleeping",
                           self.control_signal[0])

        else:
            self.log.error("Unhandled control signal received: %s",
                            self.control_signal)

        try:
            self.lock.acquire()
            self.control_signal = None
        finally:
            self.lock.release()

        return woke_up

    def _react_to_sleep_signal(self):
        self.log.debug("Received sleep signal. Going to sleep.")

        sleep_time = 0.2

        # controle loop with variable instead of break/continue commands to be
        # able to reset control_signal
        keep_checking_signal = True

        while keep_checking_signal and self.keep_running:

            if self.control_signal[0] == "SLEEP":
                # go to sleep, but check every once in
                # a while for new signals
                time.sleep(sleep_time)

                # do not reset control_signal to be able to check on it
                # reseting it woul mean "no signal set -> sleep"
                continue

            elif self.control_signal[0] == "WAKEUP":
                self.log.debug("Waking up after sleeping.")
                keep_checking_signal = False

            elif self.control_signal[0] == "EXIT":
                self.log.debug("Received %s signal.", self.control_signal[0])
                self.keep_running = False
                keep_checking_signal = False

            elif  self.control_signal[0] == b"CLOSE_SOCKETS":
                # do nothing
                pass

            else:
                self.log.debug("Received unknown control signal. "
                               "Ignoring it.")
                keep_checking_signal = False

            try:
                self.lock.acquire()
                self.control_signal = None
            finally:
                self.lock.release()

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
        # convert Windows paths
        file_id = Path(file_id).as_posix()
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

    def stop_base(self):
        self.close_socket()
        self.keep_running = False

    @abc.abstractmethod
    def stop(self):
        """Stop and clean up.
        """
        pass

    def __exit__(self, type, value, traceback):
        self.stop_base()
        self.stop()

    def __del__(self):
        self.stop_base()
        self.stop()
