from __future__ import print_function
from __future__ import unicode_literals

import os
import time
import multiprocessing
import setproctitle
import socket
import argparse

from _environment import BASE_DIR
from hidra import Transfer


class Worker(multiprocessing.Process):
    def __init__(self, worker_id, transfer_type, signal_host, target_host,
                 port, number_of_files):
        super().__init__()

        self.id = worker_id
        self.basepath = os.path.join(BASE_DIR, "data", "target")
        self.number_of_files = number_of_files

        self.port = port

        print("start Transfer on port", port)
        self.query = Transfer(transfer_type, signal_host, use_log=None)
        self.query.start([target_host, port])

        self.run()

    def run(self):
        try:
            while True:
                # print("Worker-{0}: waiting".format(self.id))
                [metadata, data] = self.query.get()
                if metadata and data:
                    self.number_of_files.value += 1
        finally:
            self.stop()

    def stop(self):
        self.query.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is running",
                        default=socket.getfqdn())
    parser.add_argument("--target_host",
                        type=str,
                        help="Host where the data should be send to",
                        default=socket.getfqdn())
    parser.add_argument("--procname",
                        type=str,
                        help="Name with which the service should be running",
                        default="test_query_speed")
    parser.add_argument("--workers",
                        type=int,
                        help="How many worker processes should be launched",
                        default=1)

    arguments = parser.parse_args()

    setproctitle.setproctitle(arguments.procname)

    workers = []

    number_of_files = multiprocessing.Value('i', 0)

    targets = []
    transfer_type = "QUERY_NEXT"

    for n in range(arguments.workers):
        p = str(50100 + n)

        w = multiprocessing.Process(target=Worker,
                                    args=(n,
                                          transfer_type,
                                          arguments.signal_host,
                                          arguments.target_host,
                                          p,
                                          number_of_files))
        workers.append(w)
        targets.append([arguments.target_host, p, 1, [".cbf"]])

    query = Transfer(transfer_type, arguments.signal_host, use_log=None)
    query.initiate(targets)

    for w in workers:
        w.start()

    try:
        while all(w.is_alive() for w in workers):
            time.sleep(0.5)
            print("number_of_files={0}".format(number_of_files.value))
    except KeyboardInterrupt:
        pass
    finally:
        print("number_of_files={0}".format(number_of_files.value))
        for w in workers:
            w.terminate()

        query.stop()


if __name__ == "__main__":
    main()
