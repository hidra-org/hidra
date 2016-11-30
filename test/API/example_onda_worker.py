import os
import sys
import time


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = os.path.join(BASE_PATH, "src", "APIs")

try:
    # search in global python modules first
    from hidra import dataTransfer
except:
    # then search in local modules
    if not API_PATH in sys.path:
        sys.path.append ( API_PATH )
    del API_PATH
    del BASE_PATH

    from hidra import dataTransfer


class worker():
    def __init__(self, id, signalHost, port):

        self.id    = id
        self.port  = port

        self.query = dataTransfer("stream", signalHost)
#        self.query = dataTransfer("queryNext", signalHost)

        print "start dataTransfer on port", str(port)
        self.query.start(port)


    def run(self):
        while True:
            try:
                print "worker-" + str(self.id) + ": waiting"
                [metadata, data] = self.query.get()
                time.sleep(0.1)
            except:
                break

            print "worker-" + str(self.id), "metadata", metadata["filename"]
        #    print "data", str(data)[:10]


    def stop(self):
        self.query.stop()


    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


if __name__ == "__main__":

    signalHost = "zitpcx19282.desy.de"
    port = "50104"

    w = worker(4, signalHost, port)

    try:
        w.run()
    finally:
        w.stop()

