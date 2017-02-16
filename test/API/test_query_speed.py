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

BASE_PATH = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__))))
API_PATH = os.path.join(BASE_PATH, "src", "APIs")
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)

try:
    # search in global python modules first
    from hidra import Transfer  # noqa F401
except:
    # then search in local modules
    if API_PATH not in sys.path:
        sys.path.append(API_PATH)

    from hidra import Transfer


class Worker(multiprocessing.Process):
    def __init__(self, id, transfer_type, basepath, signal_host, target_host, port):

        self.id = id
        self.port = port
        self.number_of_files = 0

        self.query = Transfer(transfer_type, signal_host, use_log=False)

        self.basepath = basepath

        print("start Transfer on port {0}".format(port))
        # targets are locally
        self.query.start([target_host, port])
#        self.query.start(port)

        self.run()

    def run(self):
        while True:
            try:
                print("Worker-{0}: waiting".format(self.id))
                [metadata, data] = self.query.get()
            except:
                break

            if metadata and data:
                self.number_of_files += 1

    def stop(self):
        print("number of files in worker-{0}: {1}".format(self.id, self.number_of_files))
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
    parser.add_argument("--procname",
                        type=str,
                        help="Name with which the service should be running",
                        default="test_query_speed")

    arguments = parser.parse_args()

    setproctitle.setproctitle(arguments.procname)

    signal_host = arguments.signal_host
#    signal_host = "zitpcx22614.fritz.box"
#    signal_host = "zitpcx19282.desy.de"
#    signal_host = "lsdma-lab04.desy.de"
#    signal_host = "asap3-bl-prx07.desy.de"

    #target_host = socket.gethostname()
    target_host = "zitpcx22614w.desy.de"

    transfer_type = "QUERY_NEXT"

    basepath = os.path.join(BASE_PATH, "data", "target")

    number_of_worker = 3
    workers = []

    targets = []

    for n in range(number_of_worker):
        p = str(50100 + n)

        targets.append([target_host, p, 1, [".cbf"]])

        w = multiprocessing.Process(target=Worker,
                                    args=(n,
                                          transfer_type,
                                          basepath,
                                          signal_host,
                                          target_host,
                                          p))
        workers.append(w)

    query = Transfer(transfer_type, signal_host, use_log=True)
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
