from __future__ import print_function
from __future__ import unicode_literals

import os
import time
import multiprocessing
import logging
import setproctitle
import socket
import argparse

from __init__ import BASE_PATH
import helpers

from hidra import Transfer


# enable logging
logfile_path = os.path.join(BASE_PATH, "logs")
logfile = os.path.join(logfile_path, "test_onda.log")
helpers.init_logging(logfile, True, "DEBUG")


class Worker(multiprocessing.Process):
    def __init__(self, id, transfer_type, basepath, signal_host, port):

        self.id = id
        self.port = port

        self.log = logging.getLogger("Worker-{0}".format(self.id))

        self.query = Transfer(transfer_type, signal_host, use_log=True)

        self.basepath = basepath

        self.log.debug("start Transfer on port {0}".format(port))
        # targets are locally
        self.query.start([signal_host, port])
#        self.query.start(port)

        self.run()

    def run(self):
        while True:
            try:
                self.log.debug("Worker-{0}: waiting".format(self.id))
                [metadata, data] = self.query.get()
                time.sleep(0.1)
            except:
                break

            if transfer_type in ["queryMetadata", "streamMetadata"]:
                self.log.debug("Worker-{0}: metadata {1}"
                               .format(self.id, metadata["filename"]))
                filepath = self.query.generate_target_filepath(self.basepath,
                                                               metadata)
                self.log.debug("Worker-{0}: filepath {1}"
                               .format(self.id, filepath))

                with open(filepath, "r") as file_descriptor:
                    file_descriptor.read()
                    self.log.debug("Worker-{0}: file {1} read"
                                   .format(self.id, filepath))
            else:
                print ("metadata", metadata)

            print ("data", str(data)[:100])

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
                        default="example_onda")

    arguments = parser.parse_args()

    setproctitle.setproctitle(arguments.procname)

    signal_host = arguments.signal_host
#    signal_host = "zitpcx22614.fritz.box"
#    signal_host = "zitpcx22614w.desy.de"
#    signal_host = "zitpcx19282.desy.de"
#    signal_host = "lsdma-lab04.desy.de"
#    signal_host = "asap3-bl-prx07.desy.de"

    transfer_type = "queryNext"
#    transfer_type = "stream"
#    transfer_type = "streamMetadata"
#    transfer_type = "queryMetadata"

    basepath = os.path.join(BASE_PATH, "data", "target")

    number_of_worker = 3
    workers = []

    targets = []

    for n in range(number_of_worker):
        p = str(50100 + n)

        targets.append([signal_host, p, 1, [".cbf"]])

        w = multiprocessing.Process(target=Worker,
                                    args=(n,
                                          transfer_type,
                                          basepath,
                                          signal_host,
                                          p))
        workers.append(w)

#    targets = [[signal_host, "50101", 1, [".cbf"]]]
#    targets = [[signal_host, "50101", 1],
#               [signal_host, "50102", 1],
#               [signal_host, "50103", 1]]
#    targets = [["zitpcx19282.desy.de", "50101", 1, [".cbf"]],
#               ["zitpcx19282.desy.de", "50102", 1, [".cbf"]],
#               ["zitpcx19282.desy.de", "50103", 1, [".cbf"]],
#               ["lsdma-lab04.desy.de", "50104", 1, [".cbf"]]]

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
