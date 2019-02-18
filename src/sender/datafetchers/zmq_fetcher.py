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
This module implements a data fetcher be used together with the hidra ingest
API.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple
import json
import time
import os
import zmq

from datafetcherbase import DataFetcherBase
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


IpcAddresses = namedtuple("ipc_addresses", ["datafetch"])
TcpAddresses = namedtuple("tcp_addresses", ["datafetch_bind", "datafetch_con"])
Endpoints = namedtuple("endpoints", ["datafetch_bind", "datafetch_con"])


def get_tcp_addresses(config):
    """Build the addresses used for TCP communcation.

    The addresses are only set if called on Windows. For Linux they are set
    to None.

    Args:
        config (dict): A dictionary containing the IPs to bind and to connect
                       to as well as the ports. Usually con_ip is teh DNS name.
    Returns:
        A TcpAddresses object.
    """

    if utils.is_windows():
        ext_ip = config["network"]["ext_ip"]
        con_ip = config["network"]["con_ip"]

        df_type = config["datafetcher"]["type"]
        port = config["datafetcher"][df_type]["datafetcher_port"]
        datafetch_bind = "{}:{}".format(ext_ip, port)
        datafetch_con = "{}:{}".format(con_ip, port)

        addrs = TcpAddresses(
            datafetch_bind=datafetch_bind,
            datafetch_con=datafetch_con
        )
    else:
        addrs = None

    return addrs


def get_ipc_addresses(config):
    """Build the addresses used for IPC.

    The addresses are only set if called on Linux. On windows they are set
    to None.

    Args:
        config (dict): A dictionary conaining the ipc base directory and the
                       main PID.
    Returns:
        An IpcAddresses object.
    """

    if utils.is_windows():
        addrs = None
    else:
        ipc_ip = "{}/{}".format(config["network"]["ipc_dir"],
                                config["network"]["main_pid"])

        datafetch = "{}_{}".format(ipc_ip, "datafetch")

        addrs = IpcAddresses(datafetch=datafetch)

    return addrs


def get_endpoints(ipc_addresses, tcp_addresses):
    """Configures the ZMQ endpoints depending on the protocol.

    Args:
        ipc_addresses: The addresses used for the interprocess communication
                       (ipc) protocol.
        tcp_addresses: The addresses used for communication over TCP.
    Returns:
        An Endpoints object containing the bind and connection endpoints.
    """

    if ipc_addresses is not None:
        datafetch_bind = "ipc://{}".format(ipc_addresses.datafetch)
        datafetch_con = datafetch_bind

    elif tcp_addresses is not None:
        datafetch_bind = "tcp://{}".format(tcp_addresses.datafetch_bind)
        datafetch_con = "tcp://{}".format(tcp_addresses.datafetch_con)
    else:
        msg = "Neither ipc not tcp endpoints are defined"
        raise Exception(msg)

    return Endpoints(
        datafetch_bind=datafetch_bind,
        datafetch_con=datafetch_con
    )


class DataFetcher(DataFetcherBase):
    """
    Implementation of the data fetcher to be used with the ingest API.
    """

    def __init__(self, config, log_queue, fetcher_id, context, lock):

        DataFetcherBase.__init__(self,
                                 config,
                                 log_queue,
                                 fetcher_id,
                                 "zmq_fetcher-{}".format(fetcher_id),
                                 context,
                                 lock)

        # base class sets
        #   self.config_all - all configurations
        #   self.config_df - the config of the datafetcher
        #   self.config - the module specific config
        #   self.df_type -  the name of the datafetcher module
        #   self.log_queue
        #   self.log

        self.context = context

        self.ipc_addresses = None
        self.endpoints = None

        if utils.is_windows():
            self.required_params = {
                "network": ["ext_ip"],
                "datafetcher": {self.df_type: ["datafetcher_port"]}
            }
        else:
            self.required_params = {"network": ["ipc_dir"]}

        # check that the required_params are set inside of module specific
        # config
        self.check_config()

        self._setup()

    def _setup(self):
        """
        Sets ZMQ endpoints and addresses and creates the ZMQ socket.
        """

        self.ipc_addresses = get_ipc_addresses(config=self.config_all)
        self.tcp_addresses = get_tcp_addresses(config=self.config_all)
        self.endpoints = get_endpoints(ipc_addresses=self.ipc_addresses,
                                       tcp_addresses=self.tcp_addresses)

        # Create zmq socket
        self.socket = self.start_socket(
            name="socket",
            sock_type=zmq.PULL,
            sock_con="bind",
            endpoint=self.endpoints.datafetch_bind
        )

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.
        """

        # extract event metadata
        try:
            # TODO validate metadata dict
            self.source_file = metadata["filename"]
        except:
            self.log.error("Invalid fileEvent message received.",
                           exc_info=True)
            self.log.debug("metadata=%s", metadata)
            # skip all further instructions and continue with next iteration
            raise

        # TODO combine better with source_file... (for efficiency)
        if self.config_df["local_target"]:
            self.target_file = os.path.join(self.config_df["local_target"],
                                            self.source_file)
        else:
            self.target_file = None

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
                # chunksize is coming from zmq_events

                self.log.debug("metadata = %s", metadata)
            except:
                self.log.error("Unable to assemble multi-part message.",
                               exc_info=True)
                raise

    def send_data(self, targets, metadata, open_connections):
        """Implementation of the abstract method send_data.
        """

        if not targets:
            return

        # reading source file into memory
        try:
            self.log.debug("Getting data out of queue for file '%s'...",
                           self.source_file)
            data = self.socket.recv()
        except Exception:
            self.log.error("Unable to get data out of queue for file '%s'",
                           self.source_file, exc_info=True)
            raise

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

            payload = []
            payload.append(json.dumps(metadata_extended).encode("utf-8"))
            payload.append(data)
        except Exception:
            self.log.error("Unable to pack multipart-message for file '%s'",
                           self.source_file, exc_info=True)

        # send message
        try:
            self.send_to_targets(targets=targets,
                                 open_connections=open_connections,
                                 metadata=metadata_extended,
                                 payload=payload,
                                 chunk_number=chunk_number)
            self.log.debug("Passing multipart-message for file '%s'...done.",
                           self.source_file)
        except Exception:
            self.log.error("Unable to send multipart-message for file '%s'",
                           self.source_file, exc_info=True)

    def finish(self, targets, metadata, open_connections):
        """Implementation of the abstract method finish.
        """
        pass

    def stop(self):
        """Implementation of the abstract method stop.
        """

        # close base class zmq sockets
        self.close_socket()

        # Close zmq socket
        self.stop_socket(name="socket")
