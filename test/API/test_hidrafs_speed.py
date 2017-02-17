from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import time
import multiprocessing
import setproctitle
import socket
import argparse

from __init__ import BASE_PATH
import helpers

class Worker(multiprocessing.Process):
    def __init__(self, id, port, number_of_files):

        self.id = id
        self.path = "/tmp/fs_test"
        self.number_of_files = number_of_files

        self.run()

    def run(self):
        while True:
            for f in os.listdir(self.path):
                self.number_of_files.value += 1


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

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

    number_of_worker = 3
    workers = []

    number_of_files = multiprocessing.Value('i', 0)

    for n in range(arguments.workers):
        p = str(50100 + n)

        w = multiprocessing.Process(target=Worker,
                                    args=(n, p, number_of_files))
        workers.append(w)
        w.start()

    try:
        while True:
            time.sleep(0.5)
            print ("number_of_files={0}".format(number_of_files.value))
    except KeyboardInterrupt:
        pass
    finally:
        print ("number_of_files={0}".format(number_of_files.value))
        for w in workers:
            w.terminate()
