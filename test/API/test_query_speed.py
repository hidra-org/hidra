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
    def __init__(self, id, port, number_of_files):

        self.id = id
        self.basepath = os.path.join(BASE_PATH, "data", "target")
        self.number_of_files = number_of_files

        self.port = port

#        signal_host = arguments.signal_host
        signal_host = "asap3-p00"
#        signal_host = "zitpcx22614.fritz.box"
#        signal_host = "zitpcx19282.desy.de"
#        signal_host = "lsdma-lab04.desy.de"
#        signal_host = "asap3-bl-prx07.desy.de"

        target_host = socket.gethostname()
#        target_host = "zitpcx22614w.desy.de"

        transfer_type = "QUERY_NEXT"

        self.query = Transfer(transfer_type, signal_host, use_log=None)

        print("start Transfer on port {0}".format(port))
        self.query.initiate([[target_host, self.port, 1, [".cbf"]]])
        self.query.start([target_host, port])
        # targets are locally
#        self.query.start(port)

        self.run()

    def run(self):
        try:
            while True:
                #print("Worker-{0}: waiting".format(self.id))
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

    number_of_worker = 3
    workers = []

    number_of_files = multiprocessing.Value('i', 0)

    for n in range(number_of_worker):
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

