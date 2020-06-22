from __future__ import print_function
# from __future__ import unicode_literals

import os
import zmq
import logging
import socket as socket_m
import json

from _environment import BASE_DIR
from hidra import utils


# enable logging
logfile_path = os.path.join(BASE_DIR, "logs")
logfile = os.path.join(logfile_path, "test_nexus_transfer.log")
utils.init_logging(logfile, True, "DEBUG")


class Sender (object):
    def __init__(self):
        self.ext_host = "0.0.0.0"
        self.localhost = socket_m.getfqdn()
        self.signal_port = "50050"
        self.data_port = "50100"

        self.context = zmq.Context()

        self.file_op_socket = self.context.socket(zmq.REQ)
        connection_str = ("tcp://{0}:{1}"
                          .format(self.localhost, self.signal_port))
        self.file_op_socket.connect(connection_str)
        logging.info("file_op_socket started (connect) for '{0}'"
                     .format(connection_str))

        self.data_socket = self.context.socket(zmq.PUSH)
        connection_str = ("tcp://{0}:{1}"
                          .format(self.localhost, self.data_port))
        self.data_socket.connect(connection_str)
        logging.info("data_socket started (connect) for '{0}'"
                     .format(connection_str))

    def run(self):

        source_file = os.path.join(BASE_DIR, "test_file.cbf")
        chunksize = 10485760  # 1024*1024*10 = 10MB
        filepart = 0
        timeout = 2
        filename = "test.cbf"

        message = b"OPEN_FILE"
        logging.debug("Send %s", message)
        self.file_op_socket.send_multipart([message, filename])

        recv_message = self.file_op_socket.recv_multipart()
        logging.debug("Recv confirmation: %s", recv_message)

        metadata = {
            "source_path": os.path.join(BASE_DIR, "data", "source"),
            "relative_path": "local",
            "filename": filename,
            "file_part": filepart,
            "chunksize": chunksize
        }

        # Checking if receiver is alive
#        self.data_socket.send_multipart([b"ALIVE_TEST"])
        tracker = self.data_socket.send_multipart([b"ALIVE_TEST"],
                                                  copy=False,
                                                  track=True)
        if not tracker.done:
            tracker.wait(timeout)
        logging.debug("tracker.done = %s", tracker.done)
        if not tracker.done:
            logging.error("Failed to send ALIVE_TEST", exc_info=True)
        else:
            logging.info("Sending ALIVE_TEST...success")

        # Open file
        source_fp = open(source_file, "rb")
        logging.debug("Opened file: %s", source_file)

        while True:
            # Read file content
            content = source_fp.read(chunksize)
            logging.debug("Read file content")

            if not content:
                logging.debug("break")
                break

            # Build message
            metadata["file_part"] = filepart

            payload = [json.dumps(metadata), content]

            # Send message over ZMQ
            # self.data_socket.send_multipart(payload)

            tracker = self.data_socket.send_multipart(payload,
                                                      copy=False,
                                                      track=True)
            if not tracker.done:
                logging.debug("Message part from file %s has not been sent "
                              "yet, waiting...", source_file)
                tracker.wait(timeout)
                logging.debug("Message part from file %s has not been sent "
                              "yet, waiting...done", source_file)

            logging.debug("Send")

            filepart += 1

        message = b"CLOSE_FILE"
        logging.debug("Send %s", message)
        self.file_op_socket.send(message)

        self.data_socket.send_multipart([message, filename, b"0/1"])

        recv_message = self.file_op_socket.recv()
        logging.debug("Recv confirmation: %s", recv_message)

        # Close file
        source_fp.close()
        logging.debug("Closed file: %s", source_file)

    def stop(self):
        try:
            if self.file_op_socket is not None:
                logging.info("Closing file_op_socket...")
                self.file_op_socket.close(linger=0)
                self.file_op_socket = None
            if self.data_socket is not None:
                logging.info("Closing data_socket...")
                self.data_socket.close(linger=0)
                self.data_socket = None
            if self.context is not None:
                logging.info("Destroying context...")
                self.context.destroy()
                self.context = None
                logging.info("Destroying context...done")
        except Exception:
            logging.error("Closing ZMQ Sockets...failed.", exc_info=True)

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    sender = None
    try:
        sender = Sender()
        sender.run()
    finally:
        if sender is not None:
            sender.stop()
