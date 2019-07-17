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
This module implements an example for an onda connection.
"""

# pylint: disable=too-many-function-args

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import argparse
import logging
import multiprocessing
import os
import socket
import time

import setproctitle

from _environment import BASE_DIR
from hidra import Transfer, generate_filepath
import hidra.utils as utils


class Worker(multiprocessing.Process):
    """Start a connection to hidra an get a data block.
    """

    def __init__(self,
                 identifier,
                 transfer_type,
                 basepath,
                 signal_host,
                 target_host,
                 port):

        super().__init__()

        self.identifier = identifier
        self.port = port

        self.transfer_type = transfer_type

        self.log = logging.getLogger("Worker-{}".format(self.identifier))

        self.query = Transfer(self.transfer_type, signal_host, use_log=True)

        self.basepath = basepath

        self.log.debug("start Transfer on port %s", port)
        # targets are locally
        self.query.start([target_host, port])
#        self.query.start(port)

        self.run()

    def run(self):
        while True:
            try:
                self.log.debug("Worker-%s: waiting", self.identifier)
                [metadata, data] = self.query.get()
                time.sleep(0.1)
            except Exception:
                break

            if self.transfer_type in ["QUERY_NEXT_METADATA",
                                      "STREAM_METADATA"]:
                self.log.debug("Worker-%s: metadata %s",
                               self.identifier, metadata["filename"])

                filepath = generate_filepath(self.basepath, metadata)

                self.log.debug("Worker-%s: filepath %s",
                               self.identifier, filepath)

                with open(filepath, "r") as file_descriptor:
                    file_descriptor.read()
                    self.log.debug("Worker-%s: file %s read",
                                   self.identifier, filepath)
            else:
                print("filepath", generate_filepath(self.basepath, metadata))
                print("metadata", metadata)

            print("data", str(data)[:100])

    def stop(self):
        """Clean up.
        """
        self.query.stop()

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


def main():
    """Register and start workers.
    """

    # enable logging
    logfile_path = os.path.join(BASE_DIR, "logs")
    logfile = os.path.join(logfile_path, "test_onda.log")
    utils.init_logging(logfile, True, "DEBUG")

    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is runnning",
                        default=socket.getfqdn())
    parser.add_argument("--target_host",
                        type=str,
                        help="Host where the data should be send to",
                        default=socket.getfqdn())
    parser.add_argument("--procname",
                        type=str,
                        help="Name with which the service should be running",
                        default="example_onda")

    arguments = parser.parse_args()

    setproctitle.setproctitle(arguments.procname)  # pylint: disable=no-member

    transfer_type = "QUERY_NEXT"
#    transfer_type = "STREAM"
#    transfer_type = "STREAM_METADATA"
#    transfer_type = "QUERY_NEXT_METADATA"

    basepath = os.path.join(BASE_DIR, "data", "target")

    number_of_worker = 3
    workers = []

    targets = []

    for i in range(number_of_worker):
        port = str(50200 + i)

        targets.append([arguments.target_host, port, 1, [".cbf"]])

        proc = multiprocessing.Process(target=Worker,
                                       args=(i,
                                             transfer_type,
                                             basepath,
                                             arguments.signal_host,
                                             arguments.target_host,
                                             port))
        workers.append(proc)

    query = Transfer(transfer_type, arguments.signal_host, use_log=True)
    query.initiate(targets)

    for i in workers:
        i.start()

    try:
        while True:
            pass
    except Exception:
        pass
    finally:
        for i in workers:
            i.terminate()

        query.stop()


if __name__ == "__main__":
    main()
