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


new_jobs = []
old_confirmations = []

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class CheckJobs(Base, threading.Thread):
    """
    An additional thread is needed to handle the case when confirmation
    is received from the receiver before the trigger from the DataFetcher
    arrives.
    """

    def __init__(self,
                 endpoints,
                 lock,
                 log_queue,
                 context=None):

        threading.Thread.__init__(self)

        self.log = utils.get_logger("CheckJobs", log_queue)

        self.lock = lock
        self.endpoints = endpoints

        self.run_loop = True

        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.create_sockets()

    def create_sockets(self):
        # socket to get information about data to be removed after
        # confirmation is received
        self.job_socket = self.start_socket(
            name="job_socket",
            sock_type=zmq.PULL,
            sock_con="bind",
            endpoint=self.endpoints.cleaner_job_bind
        )

        # socket to trigger cleaner to remove old confirmations
        self.cleaner_trigger_socket = self.start_socket(
            name="cleaner_trigger_socket",
            sock_type=zmq.PUSH,
            sock_con="connect",
            endpoint=self.endpoints.cleaner_trigger_con
        )

        # socket for control signals
        self.control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.control_sub_con
        )

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "signal")

        # register sockets at poller
        self.poller = zmq.Poller()
        self.poller.register(self.job_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)

    def run(self):
        global new_jobs
        global old_confirmations

        while self.run_loop:

            socks = dict(self.poller.poll())

            ######################################
            #     messages from DataFetcher      #
            ######################################
            if (self.job_socket in socks
                    and socks[self.job_socket] == zmq.POLLIN):

                self.log.debug("Waiting for job")
                message = self.job_socket.recv_multipart()
                self.log.debug("New job received: {}".format(message))

                with self.lock:
                    new_jobs.append(message)
    #                new_jobs.add(message)

                if message[1] in old_confirmations:
                    # notify cleaner
                    self.log.debug("sending cleaner trigger, message={}"
                                   .format(message))
                    self.cleaner_trigger_socket.send_multipart(message)

            ######################################
            #         control commands           #
            ######################################
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

    def stop(self):
        global new_jobs
        global old_confirmations

        self.run_loop = False

        self.stop_socket(name="job_socket")
        self.stop_socket(name="cleaner_trigger_socket")
        self.stop_socket(name="control_socket")

        if not self.ext_context and self.context is not None:
            self.log.debug("Terminating context")
            self.context.term()
            self.context = None

        with self.lock:
            new_jobs = []
            old_confirmations = []

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


# class CheckConfirmations (threading.Thread):
#    def __init__(self, conf_bind_str, lock, log_queue, context=None):
#
#        threading.Thread.__init__(self)
#
#        self.log = utils.get_logger("CheckConfirmations", log_queue)
#
#        self.lock = lock
#        self.conf_bind_str = conf_bind_str
#
#        if context:
#            self.context = context
#            self.ext_context = True
#        else:
#            self.context = zmq.Context()
#            self.ext_context = False
#
#        try:
#            self.confirmation_socket = self.context.socket(zmq.PULL)
#
#            self.confirmation_socket.connect(self.conf_bind_str)
#            self.log.info("Start confirmation_socket (connect): '{0}'"
#                          .format(self.conf_bind_str))
#        except:
#            self.log.error("Failed to start confirmation_socket (connect): "
#                           "'{0}'".format(self.conf_bind_str), exc_info=True)
#
#    def run(self):
#        global new_confirmations
#
#        self.log.debug("Waiting for confirmation")
#        message = self.confirmation_socket.recv().decode("utf-8")
#        self.log.debug("New confirmation received: {0}".format(message))
#
#        with self.lock:
#            new_confirmations.add(message)
#
#    def stop(self):
#        if self.confirmation_socket is not None:
#            self.confirmation_socket.close(0)
#            self.confirmation_socket = None
#
#        if not self.ext_context and self.context is not None:
#            self.context.destroy(0)
#            self.context = None
#
#    def __exit__(self):
#        self.stop()
#
#    def __del__(self):
#        self.stop()


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

        self.lock = threading.Lock()
        self.new_jobs = []
        self.new_confirms = []

        self.continue_run = True

        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.job_checking_thread = CheckJobs(
            endpoints=endpoints,
            lock=self.lock,
            log_queue=log_queue
        )

#        self.conf_checking_thread = CheckConfirmations(
#            self.endpoints.confirm_con,
#            self.lock,
#            log_queue,
#            context
#        )

        self.create_sockets()

        try:
            self.run()
        except KeyboardInterrupt:
            pass

    def create_sockets(self):
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

        # socket to trigger cleaner to remove old confirmations
        self.cleaner_trigger_socket = self.start_socket(
            name="cleaner_trigger_socket",
            sock_type=zmq.PULL,
            sock_con="bind",
            endpoint=self.endpoints.cleaner_trigger_bind
        )

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
        self.poller.register(self.confirmation_socket, zmq.POLLIN)
        self.poller.register(self.cleaner_trigger_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)

    def run(self):
        global new_jobs
        global old_confirmations

#        removable_elements = None

        self.job_checking_thread.start()
#        self.conf_checking_thread.start()

        while self.continue_run:
            # intersect
            # removable_elements = new_jobs & new_confirmations
            # self.log.debug("removable_elements={}"
            #               .format(removable_elements))
            #
            # for element in removable_elements:
            #    self.remove_element(element)
            #
            #    new_jobs.discard(element)
            #    new_confirmations.discard(element)
            #
            # do not loop too offen if there is nothing to process
            # if not removable_elements:
            #    time.sleep(0.1)

            socks = dict(self.poller.poll())

            ######################################
            #       messages from receiver       #
            ######################################
            if (self.confirmation_socket in socks
                    and socks[self.confirmation_socket] == zmq.POLLIN):

                self.log.debug("Waiting for confirmation")
                topic, element = self.confirmation_socket.recv_multipart()
                element = element.decode("utf-8")
                self.log.debug("topic={}".format(topic))
                self.log.debug("New confirmation received: {}".format(element))
                self.log.debug("new_jobs={}".format(new_jobs))
                self.log.debug("old_confirmations={}"
                               .format(old_confirmations))

                for base_path, file_id in new_jobs:
                    if element == file_id:
                        self.remove_element(base_path, file_id)

                        with self.lock:
                            new_jobs.remove([base_path, file_id])

                        self.log.debug("new_jobs={}".format(new_jobs))
                        continue

                with self.lock:
                    old_confirmations.append(element)
                self.log.error("confirmations without job notification "
                               "received: {}".format(element))

            ######################################
            #       messages from checkJobs      #
            ######################################
            if (self.cleaner_trigger_socket in socks
                    and socks[self.cleaner_trigger_socket] == zmq.POLLIN):

                message = self.cleaner_trigger_socket.recv_multipart()
                base_path, element = message
                self.log.debug("Received trigger to remove old confirmation")
                self.log.debug("message=[{}, {}]".format(base_path, element))
                self.remove_element(base_path, element)

                with self.lock:
                    new_jobs.remove([base_path, element])

                with self.lock:
                    old_confirmations.remove(element)

            ######################################
            #         control commands           #
            ######################################
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
        global new_jobs
        global old_confirmations

        if self.job_checking_thread is not None:
            # give control signal time to arrive
            time.sleep(0.1)

            self.log.debug("Stopping job checking thread")
            self.job_checking_thread.stop()
            self.job_checking_thread.join()
            self.job_checking_thread = None

        self.stop_socket(name="confirmation_socket")
        self.stop_socket(name="cleaner_trigger_socket")
        self.stop_socket(name="control_socket")

        if not self.ext_context and self.context is not None:
            self.log.debug("Destroying context")
            self.context.destroy(0)
            self.context = None

#        self.conf_checking_thread.stop()
#        self.conf_checking_thread.join()

        with self.lock:
            new_jobs = []
            old_confirmations = []

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()

# testing was moved into test/unittests/datafetchers/test_cleanerbase.py
