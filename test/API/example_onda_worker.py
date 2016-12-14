from __future__ import print_function
# from __future__ import unicode_literals

import time
from hidra import Transfer


class Worker():
    def __init__(self, id, signal_host, port):

        self.id = id
        self.port = port

        self.query = Transfer("stream", signal_host)
#        self.query = Transfer("queryNext", signal_host)

        print ("start Transfer on port", port)
        self.query.start(port)

    def run(self):
        while True:
            try:
                print ("worker-{0}: waiting".format(self.id))
                [metadata, data] = self.query.get()
                time.sleep(0.1)
            except:
                break

            print ("worker-{0}".format(self.id),
                   "metadata", metadata["filename"])
        #    print "data", str(data)[:10]

    def stop(self):
        self.query.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == "__main__":

    signal_host = "zitpcx19282.desy.de"
    port = "50101"

    w = Worker(4, signal_host, port)

    try:
        w.run()
    finally:
        w.stop()
