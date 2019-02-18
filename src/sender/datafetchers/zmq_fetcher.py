from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import zmq
import os
import json
import time
from collections import namedtuple

from datafetcherbase import DataFetcherBase
import utils

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
        ext_ip = config["ext_ip"]
        con_ip = config["con_ip"]

        port = config["data_fetcher_port"]
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
        ipc_ip = "{}/{}".format(config["ipc_dir"], config["main_pid"])

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

    def __init__(self, config, log_queue, fetcher_id, context, lock):

        DataFetcherBase.__init__(self,
                                 config,
                                 log_queue,
                                 fetcher_id,
                                 "zmq_fetcher-{}".format(fetcher_id),
                                 context,
                                 lock)

        self.config = config
        self.log_queue = log_queue
        self.context = context

        self.ipc_addresses = None
        self.endpoints = None

        if utils.is_windows():
            self.required_params = ["ext_ip", "data_fetcher_port"]
        else:
            self.required_params = ["ipc_dir"]

        self.check_config
        self.setup()

    def setup(self):
        """
        Sets ZMQ endpoints and addresses and creates the ZMQ socket.
        """

        self.ipc_addresses = get_ipc_addresses(config=self.config)
        self.tcp_addresses = get_tcp_addresses(config=self.config)
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
            self.log.debug("metadata={}".format(metadata))
            # skip all further instructions and continue with next iteration
            raise

        # TODO combine better with source_file... (for efficiency)
        if self.config["local_target"]:
            self.target_file = os.path.join(self.config["local_target"],
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

                self.log.debug("metadata = {}".format(metadata))
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
            self.log.debug("Getting data out of queue for file '{}'..."
                           .format(self.source_file))
            data = self.socket.recv()
        except:
            self.log.error("Unable to get data out of queue for file '{}'"
                           .format(self.source_file), exc_info=True)
            raise

    #    try:
    #        chunksize = metadata["chunksize"]
    #    except:
    #        self.log.error("Unable to get chunksize", exc_info=True)

        try:
            self.log.debug("Packing multipart-message for file {}..."
                           .format(self.source_file))
            chunk_number = 0

            # assemble metadata for zmq-message
            metadata_extended = metadata.copy()
            metadata_extended["chunk_number"] = chunk_number

            payload = []
            payload.append(json.dumps(metadata_extended).encode("utf-8"))
            payload.append(data)
        except:
            self.log.error("Unable to pack multipart-message for file '{}'"
                           .format(self.source_file), exc_info=True)

        # send message
        try:
            self.send_to_targets(targets,
                                 open_connections,
                                 metadata_extended,
                                 payload,
                                 chunk_number)
            self.log.debug("Passing multipart-message for file '{}'...done."
                           .format(self.source_file))
        except:
            self.log.error("Unable to send multipart-message for file '{}'"
                           .format(self.source_file), exc_info=True)

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
