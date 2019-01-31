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
This module implements the data fetcher base class from which all data fetchers
inherit from.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import abc
import json
import os
import sys
import zmq

#import __init__ as init  # noqa F401 # pylint: disable=unused-import
from base_class import Base
import hidra.utils as utils
from hidra.utils import WrongConfiguration

# source:
# pylint: disable=line-too-long
# http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5  # noqa E501
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC  # pylint: disable=no-member
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataHandlingError(Exception):
    """An exception class to be used when handling data.
    """
    pass


class DataFetcherBase(Base, ABC):
    """
    Implementation of the data fetcher base class.
    """

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
        super(DataFetcherBase, self).__init__()

        self.log_queue = log_queue
        self.log = utils.get_logger(logger_name, self.log_queue)

        self.config_all = config

        # base_parameters
        self.required_params_base = {
            "network": [
                "endpoints",
                "main_pid"
            ],
            "datafetcher": [
                "type",
                "chunksize",
                "local_target",
                ["remove_data", [True,
                                 False,
                                 "stop_on_error",
                                 "with_confirmation"]]
            ]
        }

        self.required_params_dep = {}
        self.config_reduced = {}
        self._base_check(module_class="datafetcher")

        self.config_df = self.config_all["datafetcher"]
        self.df_type = self.config_df["type"]
        if self.required_params_dep:
            self.config = self.config_df[self.df_type]
        else:
            self.config = {}

        self.fetcher_id = fetcher_id
        self.context = context
        self.cleaner_job_socket = None

        self.source_file = None
        self.target_file = None

        self.required_params = []

        self.base_setup()

    def check_config(self, print_log=False, check_module_config=True):
        """Check that the configuration containes the nessessary parameters.

        Args:
            print_log (boolean, optional): If a summary of the configured
                                           parameters should be logged.
        Raises:
            WrongConfiguration: The configuration has missing or
                                wrong parameteres.
        """

        if isinstance(self.required_params, list):
            self.required_params = {
                "datafetcher": {self.df_type: self.required_params}
            }

        if check_module_config:
            required_params=[
                self.required_params_base,
                self.required_params_dep,
                self.required_params
            ]
        else:
            required_params=[
                self.required_params_base,
            ]

        config_reduced = self._check_config_base(
            config=self.config_all,
            required_params=required_params
        )

        self.config_reduced.update(config_reduced)

        if print_log:
            self.log.info("Configuration for data fetcher %s: %s",
                          self.df_type, self.config_reduced)

    def base_setup(self):
        """Sets up the shared components needed by all datafetchers.

        Created a socket to communicate with the cleaner and sets the topic.
        """

        if self.config_df["remove_data"] == "with_confirmation":

            config_net = self.config_all["network"]

            # create socket
            self.cleaner_job_socket = self.start_socket(
                name="cleaner_job_socket",
                sock_type=zmq.PUSH,
                sock_con="connect",
                endpoint=config_net["endpoints"].cleaner_job_con
            )

            self.confirmation_topic = (
                utils.generate_sender_id(config_net["main_pid"])
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
                        self.log.info("Sending message part from file '%s' "
                                      "to '%s' with priority %s",
                                      self.source_file, target, prio)

                    elif send_type == "metadata":
                        # json.dumps(None) is 'N.'
                        tracker = open_connections[target].send_multipart(
                            [json.dumps(metadata).encode("utf-8"),
                             json.dumps(None).encode("utf-8")],
                            copy=False,
                            track=True
                        )
                        self.log.info("Sending metadata of message part from "
                                      "file '%s' to '%s' with priority %s",
                                      self.source_file, target, prio)
                        self.log.debug("metadata=%s", metadata)

                    if not tracker.done:
                        self.log.debug("Message part from file '%s' has not "
                                       "been sent yet, waiting...",
                                       self.source_file)
                        tracker.wait(timeout)
                        self.log.debug("Message part from file '%s' has not "
                                       "been sent yet, waiting...done",
                                       self.source_file)

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
                    self.log.info("Sending message part from file '%s' to "
                                  "'%s' with priority %s",
                                  self.source_file, target, prio)

                elif send_type == "metadata":
                    open_connections[target].send_multipart(
                        [json.dumps(metadata).encode("utf-8"),
                         json.dumps(None).encode("utf-8")],
                        zmq.NOBLOCK
                    )
                    self.log.info("Sending metadata of message part from file "
                                  "'%s' to '%s' with priority %s",
                                  self.source_file, target, prio)
                    self.log.debug("metadata=%s", metadata)

    # pylint: disable=no-self-use
    def generate_file_id(self, metadata):
        """Generates a file id consisting of relative path and file name

        Args:
            metadata (dict): The dictionary with the metedata of the file.
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
        """Close open sockets
        """
        self.stop_socket("cleaner_job_socket")

    @abc.abstractmethod
    def stop(self):
        """Stop and clean up.
        """
        pass

    def __exit__(self, exception_type, exception_value, traceback):
        self.close_socket()
        self.stop()

    def __del__(self):
        self.close_socket()
        self.stop()
