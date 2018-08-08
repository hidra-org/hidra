from __future__ import print_function
# from __future__ import unicode_literals

import os
import zmq
import logging
import threading
import json
import tempfile

from __init__ import BASE_DIR
import utils

from hidra import Transfer
from hidra import Ingest


# enable logging
logfile_path = os.path.join(BASE_DIR, "logs")
logfile = os.path.join(logfile_path, "test_nexus_ingest_with_transfer.log")
utils.init_logging(logfile, True, "DEBUG")

print("\n==== TEST: hidraIngest together with nexus transfer ====\n")


class HidraSimulation (threading.Thread):
    def __init__(self, context=None):
        self.ext_host = "0.0.0.0"
        self.localhost = "zitpcx19282"
#        self.localhost = "localhost"
        self.dataOutPort = "50100"

        self.log = logging.getLogger("HidraSimulation")

        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.event_socket = self.context.socket(zmq.PULL)
        connection_str = "ipc://{0}".format(os.path.join(tempfile.gettempdir(),
                                                         "hidra",
                                                         "eventDet"))
#        connection_str = ("tcp://{0}:{1}"
#                          .format(self.ext_host, self.event_port))
        self.event_socket.bind(connection_str)
        self.log.info("event_socket started (bind) for '{0}'"
                      .format(connection_str))

        self.data_in_socket = self.context.socket(zmq.PULL)
        connection_str = "ipc://{0}".format(os.path.join(tempfile.gettempdir(),
                                                         "hidra",
                                                         "dataFetch"))
#        connection_str = ("tcp://{0}:{1}"
#                          .format(self.ext_host, self.dataInPort))
        self.data_in_socket.bind(connection_str)
        self.log.info("data_in_socket started (bind) for '{0}'"
                      .format(connection_str))

        self.data_out_socket = self.context.socket(zmq.PUSH)
        connection_str = ("tcp://{0}:{1}"
                          .format(self.localhost, self.dataOutPort))
        self.data_out_socket.connect(connection_str)
        self.log.info("data_out_socket started (connect) for '{0}'"
                      .format(connection_str))

        self.poller = zmq.Poller()
        self.poller.register(self.event_socket, zmq.POLLIN)
        self.poller.register(self.data_in_socket, zmq.POLLIN)

        threading.Thread.__init__(self)

    def run(self):
        filename = "1.h5"

        while True:
            try:
                socks = dict(self.poller.poll())
                data_message = None
                metadata = None

                if (socks
                        and self.event_socket in socks
                        and socks[self.event_socket] == zmq.POLLIN):

                    metadata = self.event_socket.recv()
                    self.log.debug("event_socket recv: {0}".format(metadata))

                    if metadata == b"CLOSE_FILE":
                        self.data_out_socket.send_multipart(
                            [metadata, filename, "0/1"])

                if (socks
                        and self.data_in_socket in socks
                        and socks[self.data_in_socket] == zmq.POLLIN):

                    data = self.data_in_socket.recv()
                    self.log.debug("data_socket recv: {0}".format(data))

                    data_message = [json.dumps(metadata), data]

                    self.data_out_socket.send_multipart(data_message)
                    self.log.debug("Send")

            except zmq.ZMQError as e:
                if not str(e) == "Socket operation on non-socket":
                    self.log.error("Error in run", exc_info=True)
                break
            except:
                self.log.error("Error in run", exc_info=True)
                break

    def stop(self):
        try:
            if self.event_socket:
                self.log.info("closing event_socket...")
                self.event_socket.close(linger=0)
                self.event_socket = None
            if self.data_in_socket:
                self.log.info("closing data_in_socket...")
                self.data_in_socket.close(linger=0)
                self.data_in_socket = None
            if self.data_out_socket:
                self.log.info("closing data_out_socket...")
                self.data_out_socket.close(linger=0)
                self.data_out_socket = None
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        if not self.ext_context and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)


def hidra_ingest(numb_to_send):
    obj = Ingest(use_log=True)

    obj.create_file("1.h5")

    for i in range(numb_to_send):
        try:
            data = "THISISTESTDATA-{0}".format(i)
            obj.write(data)
            logging.info("write")
        except:
            logging.error("hidra_ingest break", exc_info=True)
            break

    try:
        obj.close_file()
    except:
        logging.error("Could not close file", exc_info=True)

    obj.stop()


def open_callback(params, retrieved_params):
    print("open_callback", params, retrieved_params)


def close_callback(params, retrieved_params):
    params["run_loop"] = False
    print("close_callback", params, retrieved_params)


def read_callback(params, retrieved_params):
    print("read_callback", params, retrieved_params)


def nexus_transfer():
    obj = Transfer("NEXUS", use_log=True)
    obj.start(["zitpcx19282", "50100"])
#    obj.start(["localhost", "50100"])

    callback_params = {
        "run_loop": True
    }

    # number to receive + open signal + close signal
    try:
        while callback_params["run_loop"]:
            try:
                obj.read(callback_params, open_callback, read_callback,
                         close_callback)
            except KeyboardInterrupt:
                break
            except:
                logging.error("nexus_transfer break", exc_info=True)
                break
    finally:
        obj.stop()


if __name__ == "__main__":
    use_test = True
    # use_test = False

    if use_test:
        hidra_simulation_thread = HidraSimulation()
        hidra_simulation_thread.start()

    number = 5

    hidra_ingest_thread = threading.Thread(target=hidra_ingest,
                                           args=(number, ))
    nexus_transfer_thread = threading.Thread(target=nexus_transfer)

    hidra_ingest_thread.start()
    nexus_transfer_thread.start()

    hidra_ingest_thread.join()
    nexus_transfer_thread.join()

    if use_test:
        hidra_simulation_thread.stop()

    print("\n==== TEST END: hidraIngest together with nexus transfer ====\n")
