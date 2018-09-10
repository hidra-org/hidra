from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import zmq
import threading
import utils
import sys
import time
import abc

import __init__  as init # noqa F401  # rename it to remove F811
from base_class import Base

# source:
# http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5  # noqa E501
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class CleanerBase(Base, ABC):
    def __init__(self,
                 config,
                 log_queue,
                 endpoints,
                 context=None):
        """

        Args:
             config:
             log_queue:
             endpoints: ZMQ endpoints to use
             context (optional): ZMQ context to use

        """

        self.log = utils.get_logger("Cleaner", log_queue)

        self.config = config
        self.endpoints = endpoints

        self.continue_run = True

        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.create_sockets()

        try:
            self.run()
        except KeyboardInterrupt:
            pass

    def create_sockets(self):

        # socket to get information about data to be removed after
        # confirmation is received
        self.job_socket = self.start_socket(
            name="job_socket",
            sock_type=zmq.PULL,
            sock_con="bind",
            endpoint=self.endpoints.cleaner_job_bind
        )

        # socket to receive confirmation that data can be removed/discarded
        self.confirmation_socket = self.start_socket(
            name="confirmation_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.confirm_con
        )

        topic = utils.generate_sender_id(self.config["main_pid"])
        # topic = b"test"
        self.confirmation_socket.setsockopt(zmq.SUBSCRIBE, topic)

        # socket for control signals
        self.control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.control_sub_con
        )

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")

        # register sockets at poller
        self.poller = zmq.Poller()
        self.poller.register(self.job_socket, zmq.POLLIN)
        self.poller.register(self.confirmation_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)

    def run(self):
        confirmations = {}
        jobs = {}

        while self.continue_run:
            socks = dict(self.poller.poll())

            # ----------------------------------------------------------------
            # messages from receiver
            # ----------------------------------------------------------------
            if (self.confirmation_socket in socks
                    and socks[self.confirmation_socket] == zmq.POLLIN):

                self.log.debug("Waiting for confirmation")
                message = self.confirmation_socket.recv_multipart()

                topic = message[0]
                file_id = message[1].decode("utf-8")

                # backward compatibility with versions <= 4.0.7
                if len(message) > 2:
                    chunk_number = message[2]
                else:
                    chunk_number = None

                self.log.debug("topic={}".format(topic))
                self.log.debug("New confirmation received: {}".format(file_id))
                self.log.debug("chunk_number={}".format(chunk_number))

                if file_id in confirmations and chunk_number != 1:
                    confirmations[file_id]["count"] += 1
                    confirmations[file_id]["chunks"].append(chunk_number)
                else:
                    # either new confirmation or
                    # file transfer was aborted and restarted -> reset chunks
                    confirmations[file_id] = {
                        "count": 1,
                        "chunks": [chunk_number]
                    }

                self.log.debug("jobs={}".format(jobs))
                #self.log.debug("confirmations={}".format(confirmations))

                if file_id in jobs:
                    this_confirm = confirmations[file_id]
                    this_job = jobs[file_id]

                    if this_confirm["count"] >= this_job["n_chunks"]:

                        # check that all chunks were received:
                        received_chunks = list(set(this_confirm["chunks"]))
                        if (len(received_chunks) == this_job["n_chunks"]
                                # backward compatibility with versions <= 4.0.7
                                or received_chunks == [None]):
                            self.remove_element(this_job["base_path"], file_id)
                            del confirmations[file_id]
                            del jobs[file_id]
                        else:
                            # correct count
                            self.log.info("More confirmations received than "
                                          "chunks sent for file {}."
                                          .format(file_id))
                            self.log.debug("chunks received={}"
                                           .format(this_confirm["chunks"]))

                            this_confirm["count"] = len(received_chunks)
                            this_confirm["chunks"] = received_chunks
                else:
                    self.log.debug("confirmations without job notification "
                                   "received: {}".format(file_id))

            # ----------------------------------------------------------------
            # messages from DataFetcher
            # ----------------------------------------------------------------
            if (self.job_socket in socks
                    and socks[self.job_socket] == zmq.POLLIN):

                self.log.debug("Waiting for job")
                message = self.job_socket.recv_multipart()
                self.log.debug("New job received: {}".format(message))

                base_path, file_id, n_chunks = message
                n_chunks = int(n_chunks)

                if (file_id in confirmations
                        and confirmations[file_id]["count"] >= n_chunks):

                    this_confirm = confirmations[file_id]

                    # check that all chunks were received:
                    received_chunks = list(set(this_confirm["chunks"]))
                    if (len(received_chunks) == n_chunks
                            # backward compatibility with versions <= 4.0.7
                            or received_chunks == [None]):
                        self.remove_element(base_path, file_id)
                        del confirmations[file_id]
                        del jobs[file_id]
                    else:
                        # correct count
                        self.log.info("More confirmations received than "
                                      "chunks sent for file {}."
                                      .format(file_id))
                        self.log.debug("chunks received={}"
                                       .format(this_confirm["chunks"]))

                        this_confirm["count"] = len(received_chunks)
                        this_confirm["chunks"] = received_chunks
                else:
                    jobs[file_id] = {
                        "base_path": base_path,
                        "n_chunks": n_chunks
                    }

            # ----------------------------------------------------------------
            # control commands
            # ----------------------------------------------------------------
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):
                try:
                    message = self.control_socket.recv_multipart()
                    self.log.debug("Control signal received")
                    self.log.debug("message = {}".format(message))
                except:
                    self.log.error("Receiving control signal...failed",
                                   exc_info=True)
                    continue

                # remove subsription topic
                del message[0]

                if message[0] == b"SLEEP":
                    self.log.debug("Received sleep signal")
                    continue
                elif message[0] == b"WAKEUP":
                    self.log.debug("Received wakeup signal")
                    # Wake up from sleeping
                    continue
                elif message[0] == b"EXIT":
                    self.log.debug("Received exit signal")
                    break
                else:
                    self.log.error("Unhandled control signal received: {}"
                                   .format(message))

    @abc.abstractmethod
    def remove_element(self, base_path, source_file_id):
        pass

    def stop(self):

        self.stop_socket(name="job_socket")
        self.stop_socket(name="confirmation_socket")
        self.stop_socket(name="control_socket")

        if not self.ext_context and self.context is not None:
            self.log.debug("Destroying context")
            self.context.destroy(0)
            self.context = None

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()

# testing was moved into test/unittests/datafetchers/test_cleanerbase.py
