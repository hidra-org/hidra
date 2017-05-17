from __future__ import print_function
# from __future__ import unicode_literals

import os
import zmq
import logging
import threading
import json

from __init__ import BASE_PATH
import helpers

from hidra import Transfer


# enable logging
logfile_path = os.path.join(os.path.join(BASE_PATH, "logs"))
logfile = os.path.join(logfile_path, "example_nexus_transfer.log")
helpers.init_logging(logfile, True, "DEBUG")

print("\n==== TEST: nexus transfer ====\n")


class SenderAsThread (threading.Thread):
    def __init__(self):
        self.ext_host = "0.0.0.0"
#        self.localhost = "localhost"
        self.signal_host = "zitpcx19282"
        self.signal_port = "50050"
        self.data_port = "50100"

        self.context = zmq.Context()

        self.file_op_socket = self.context.socket(zmq.REQ)
        connection_str = ("tcp://{0}:{1}"
                          .format(self.signal_host, self.signal_port))
        self.file_op_socket.connect(connection_str)
        logging.info("file_op_socket started (connect) for '{0}'"
                     .format(connection_str))

        self.data_socket = self.context.socket(zmq.PUSH)
        connection_str = ("tcp://{0}:{1}"
                          .format(self.signal_host, self.data_port))
        self.data_socket.connect(connection_str)
        logging.info("data_socket started (connect) for '{0}'"
                     .format(connection_str))

        threading.Thread.__init__(self)

    def run(self):
        filename = "1.h5"
        self.file_op_socket.send_multipart(
            [b"OPEN_FILE", filename.encode("utf-8")])

        recv_message = self.file_op_socket.recv_multipart()
        logging.debug("Recv confirmation: {0}".format(recv_message))

        for i in range(5):
            metadata = {
                "source_path": os.path.join(BASE_PATH, "data", "source"),
                "relative_path": "local",
                "filename": filename,
                "filepart": "{0}".format(i)
            }
            metadata = json.dumps(metadata).encode("utf-8")

            data = b"THISISTESTDATA-{0}".format(i)

            data_message = [metadata, data]
            self.data_socket.send_multipart(data_message)
            logging.debug("Send")

        message = b"CLOSE_FILE"
        logging.debug("Send {0}".format(message))
        self.file_op_socket.send(message)

        self.data_socket.send_multipart([message, filename, "0/1"])

        recv_message = self.file_op_socket.recv()
        logging.debug("Recv confirmation: {0}".format(recv_message))

    def stop(self):
        try:
            if self.file_op_socket:
                logging.info("Closing file_op_socket...")
                self.file_op_socket.close(linger=0)
                self.file_op_socket = None
            if self.data_socket:
                logging.info("Closing data_socket...")
                self.data_socket.close(linger=0)
                self.data_socket = None
            if self.context:
                logging.info("Destroying context...")
                self.context.destroy()
                self.context = None
                logging.info("Destroying context...done")
        except:
            logging.error("Closing ZMQ Sockets...failed.", exc_info=True)

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


def open_callback(params, retrieved_params):
    print(params, retrieved_params)


def close_callback(params, retrieved_params):
    params["run_loop"] = False
    print(params, retrieved_params)


def read_callback(params, retrieved_params):
    print(params, retrieved_params)


senderThread = SenderAsThread()
senderThread.start()

obj = Transfer("NEXUS", use_log=True)
obj.start(["zitpcx19282", "50100"])

callback_params = {
    "run_loop": True
}

try:
    while callback_params["run_loop"]:
        try:
            obj.read(callback_params, open_callback, read_callback,
                     close_callback)
        except KeyboardInterrupt:
            break
        except:
            logging.error("break", exc_info=True)
            break
finally:
    senderThread.stop()
    obj.stop()

print("\n==== TEST END: nexus transfer ====\n")
