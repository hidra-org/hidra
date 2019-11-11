from __future__ import print_function
from __future__ import unicode_literals

import os
import zmq
import logging
# import json
import tempfile

from _environment import BASE_DIR
import utils


# enable logging
logfile_path = os.path.join(BASE_DIR, "logs")
logfile = os.path.join(logfile_path, "test_ingest.log")
utils.init_logging(logfile, True, "DEBUG")


class Receiver (object):
    def __init__(self, context=None):
        self.ext_host = "0.0.0.0"
        self.signal_port = "50050"
        self.event_port = "50003"
        self.data_port = "50010"

        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.signal_socket = None
        self.event_socket = None
        self.data_socket = None

        self.signal_socket = self.context.socket(zmq.REP)
        connection_str = ("tcp://{0}:{1}"
                          .format(self.ext_host, self.signal_port))
        self.signal_socket.bind(connection_str)
        logging.info("signal_socket started (bind) for '%s'", connection_str)

        self.event_socket = self.context.socket(zmq.PULL)
        connection_str = ("ipc://{0}"
                          .format(os.path.join(tempfile.gettempdir(),
                                               "hidra",
                                               "eventDet")))
        self.event_socket.bind(connection_str)
        logging.info("event_socket started (bind) for '%s'", connection_str)

        self.data_socket = self.context.socket(zmq.PULL)
        connection_str = ("ipc://{0}"
                          .format(os.path.join(tempfile.gettempdir(),
                                               "hidra",
                                               "dataFetch")))
        self.data_socket.bind(connection_str)
        logging.info("data_socket started (bind) for '%s", connection_str)

        self.poller = zmq.Poller()
        self.poller.register(self.signal_socket, zmq.POLLIN)
        self.poller.register(self.event_socket, zmq.POLLIN)
        self.poller.register(self.data_socket, zmq.POLLIN)

    def run(self):
        file_descriptor = None
        filename = os.path.join(BASE_DIR,
                                "data",
                                "target",
                                "local",
                                "test.cbf")

        mark_as_close = False
        all_closed = False
        while True:
            try:
                socks = dict(self.poller.poll())

                if (socks
                        and self.signal_socket in socks
                        and socks[self.signal_socket] == zmq.POLLIN):

                    message = self.signal_socket.recv_multipart()
                    logging.debug("signal_socket recv: %s", message)

                    if message[0] == b"OPEN_FILE":
                        # Open file
                        file_descriptor = open(filename, "wb")
                        logging.debug("Opened file")

                        self.signal_socket.send_multipart(message)
                        logging.debug("signal_socket send: %s", message)

                    elif message[0] == b"CLOSE_FILE":
                        if all_closed:
                            self.signal_socket.send_multipart(message)
                            logging.debug("signal_socket send: %s", message)

                            # Close file
                            file_descriptor.close()
                            logging.debug("Closed file")

                            break
                        else:
                            mark_as_close = message
                    else:
                        self.signal_socket.send_multipart(message)
                        logging.debug("signal_socket send: %s", message)

                if (socks
                        and self.event_socket in socks
                        and socks[self.event_socket] == zmq.POLLIN):

                    event_message = self.event_socket.recv()
#                    logging.debug("event_socket recv: %s",
#                                  json.loads(event_message))
                    logging.debug("event_socket recv: %s", event_message)

                    if event_message == b"CLOSE_FILE":
                        if mark_as_close:
                            self.signal_socket.send_multipart(mark_as_close)
                            logging.debug("signal_socket send: %s",
                                          mark_as_close)
                            mark_as_close = None

                            # Close file
                            file_descriptor.close()
                            logging.debug("Closed file")

                            break
                        else:
                            all_closed = True

                if (socks
                        and self.data_socket in socks
                        and socks[self.data_socket] == zmq.POLLIN):

                    data = self.data_socket.recv()

                    logging.debug("data_socket recv (len=%s): %s",
                                  data[:100], len(data))

                    file_descriptor.write(data)
                    logging.debug("Write file content")

            except Exception:
                logging.error("Exception in run", exc_info=True)
                break

    def stop(self):
        try:
            if self.signal_socket:
                logging.info("closing signal_socket...")
                self.signal_socket.close(linger=0)
                self.signal_socket = None
            if self.event_socket:
                logging.info("closing event_socket...")
                self.event_socket.close(linger=0)
                self.event_socket = None
            if self.data_socket:
                logging.info("closing data_socket...")
                self.data_socket.close(linger=0)
                self.data_socket = None
        except Exception:
            logging.error("closing ZMQ Sockets...failed.", exc_info=True)

        if not self.ext_context and self.context:
            try:
                logging.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                logging.info("Closing ZMQ context...done.")
            except Exception:
                logging.error("Closing ZMQ context...failed.", exc_info=True)

    def __del__(self):
        self.stop()

    def __exit__(self):
        self.stop()


if __name__ == '__main__':
    r = Receiver()
    try:
        r.run()
    finally:
        r.stop()
