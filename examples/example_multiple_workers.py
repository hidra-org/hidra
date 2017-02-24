from __future__ import print_function
from __future__ import unicode_literals

import os
import multiprocessing
import socket
import argparse

import __init__
from hidra import Transfer


class Worker(multiprocessing.Process):
    def __init__(self, id, transfer_type, signal_host, target_host, port):

        self.id = id
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
                print("Worker-{0}: waiting".format(self.id))
                [metadata, data] = self.query.get()
            except:
                break

            print("metadata", metadata)
            print("data", str(data)[:100])

    def stop(self):
        self.query.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is runnning",
                        default=socket.gethostname())
    parser.add_argument("--target_host",
                    type=str,
                    help="Host where the data should be send to",
                    default=socket.gethostname())
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
    for n in range(number_of_worker):
        p = str(50100 + n)

        targets.append([arguments.target_host, p, 1, [".cbf"]])

        w = multiprocessing.Process(target=Worker,
                                    args=(n,
                                          transfer_type,
                                          arguments.signal_host,
                                          arguments.target_host,
                                          p))
        workers.append(w)

    # register these workers on the sending side
    # this is done from the master to enforce that the data received from the workers is disjuct
    query = Transfer(transfer_type, arguments.signal_host, use_log=False)
    query.initiate(targets)

    for w in workers:
        w.start()

    try:
        while True:
            pass
    except:
        pass
    finally:
        for w in workers:
            w.terminate()

        query.stop()
