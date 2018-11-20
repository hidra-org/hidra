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
This module implements an example working with multiple workers.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import multiprocessing
import socket

import _environment  # pylint: disable=unused-import
from hidra import Transfer


class Worker(multiprocessing.Process):
    """Start a connection to hidra an get a data block.
    """

    def __init__(self,
                 identifier,
                 transfer_type,
                 signal_host,
                 target_host,
                 port):  # noqa F811

        super(Worker, self).__init__()

        self.identifier = identifier
        self.port = port

        self.query = Transfer(transfer_type, signal_host, use_log=False)

        # Set up ZeroMQ for this worker
        print("start Transfer on port {0}".format(port))
        self.query.start([target_host, port])

        self.run()

    def run(self):
        while True:
            try:
                # Get new data
                print("Worker-{0}: waiting".format(self.identifier))
                [metadata, data] = self.query.get()
            except Exception:
                break

            print("metadata", metadata)
            print("data", str(data)[:100])

    def stop(self):
        """Clean up:
        """
        self.query.stop()

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


def main():
    """Register and start workers.
    """

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

    transfer_type = "QUERY_NEXT"

    number_of_worker = 3
    workers = []

    targets = []

    # Create <number_of_worker> workers to receive and process data
    for i in range(number_of_worker):
        port = str(50100 + i)

        targets.append([arguments.target_host, port, 1, [".cbf"]])

        proc = multiprocessing.Process(target=Worker,
                                       args=(i,
                                             transfer_type,
                                             arguments.signal_host,
                                             arguments.target_host,
                                             port))
        workers.append(proc)

    # register these workers on the sending side
    # this is done from the master to enforce that the data received from the
    # workers is disjoint
    query = Transfer(transfer_type, arguments.signal_host, use_log=False)
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
