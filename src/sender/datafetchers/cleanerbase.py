from __future__ import unicode_literals

import zmq
import threading
from __init__ import BASE_PATH
import utils
import os
import sys
import time
import abc

# source:
# http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5  # noqa E501
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})


new_jobs = []
# new_jobs = set()
# new_confirmations = set()

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class CheckJobs (threading.Thread):
    def __init__(self, job_bind_str, control_con_str, lock, log_queue,
                 context=None):

        threading.Thread.__init__(self)

        self.log = utils.get_logger("CheckJobs", log_queue)

        self.lock = lock
        self.job_bind_str = job_bind_str
        self.control_con_str = control_con_str

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
        try:
            self.job_socket = self.context.socket(zmq.PULL)
            self.job_socket.bind(self.job_bind_str)
            self.log.info("Start job_socket (bind): '{0}'"
                          .format(self.job_bind_str))
        except:
            self.log.error("Failed to start job_socket (bind): '{0}'"
                           .format(self.job_bind_str), exc_info=True)

        # socket for control signals
        try:
            self.control_socket = self.context.socket(zmq.SUB)
            self.control_socket.connect(self.control_con_str)
            self.log.info("Start control_socket (connect): '{0}'"
                          .format(self.control_con_str))
        except:
            self.log.error("Failed to start control_socket (connect): '{0}'"
                           .format(self.control_con_str), exc_info=True)
            raise

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "signal")

        # register sockets at poller
        self.poller = zmq.Poller()
        self.poller.register(self.job_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)

    def run(self):
        global new_jobs

        while self.run_loop:

            socks = dict(self.poller.poll())

            ######################################
            #     messages from DataFetcher      #
            ######################################
            if (self.job_socket in socks
                    and socks[self.job_socket] == zmq.POLLIN):

                self.log.debug("Waiting for job")
                message = self.job_socket.recv_multipart()
                self.log.debug("New job received: {0}".format(message))

                self.lock.acquire()
                new_jobs.append(message)
#                new_jobs.add(message)
                self.lock.release()

            ######################################
            #         control commands           #
            ######################################
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):
                try:
                    message = self.control_socket.recv_multipart()
                    self.log.debug("Control signal received")
                    self.log.debug("message = {0}".format(message))
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
                    self.log.error("Unhandled control signal received: {0}"
                                   .format(message))

    def stop(self):
        if self.job_socket is not None:
            self.job_socket.close(0)
            self.job_socket = None

        if self.control_socket is not None:
            self.control_socket.close(0)
            self.control_socket = None

        if not self.ext_context and self.context is not None:
            self.context.destroy(0)
            self.context = None

        self.run_loop = False

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()

"""
class CheckConfirmations (threading.Thread):
    def __init__(self, conf_bind_str, lock, log_queue, context=None):

        threading.Thread.__init__(self)

        self.log = utils.get_logger("CheckConfirmations", log_queue)

        self.lock = lock
        self.conf_bind_str = conf_bind_str

        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        try:
            self.confirmation_socket = self.context.socket(zmq.PULL)

            self.confirmation_socket.connect(self.conf_bind_str)
            self.log.info("Start confirmation_socket (connect): '{0}'"
                          .format(self.conf_bind_str))
        except:
            self.log.error("Failed to start confirmation_socket (connect): "
                           "'{0}'".format(self.conf_bind_str), exc_info=True)

    def run(self):
        global new_confirmations

        self.log.debug("Waiting for confirmation")
        message = self.confirmation_socket.recv().decode("utf-8")
        self.log.debug("New confirmation received: {0}".format(message))

        self.lock.acquire()
        new_confirmations.add(message)
        self.lock.release()

    def stop(self):
        if self.confirmation_socket is not None:
            self.confirmation_socket.close(0)
            self.confirmation_socket = None

        if not self.ext_context and self.context is not None:
            self.context.destroy(0)
            self.context = None

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
"""


class CleanerBase(ABC):
    def __init__(self, config, log_queue, job_bind_str, conf_bind_str,
                 control_con_str, context=None):

        self.log = utils.get_logger("Cleaner", log_queue)

        self.config = config
        self.job_bind_str = job_bind_str
        self.conf_bind_str = conf_bind_str
        self.control_con_str = control_con_str

        self.lock = threading.Lock()
        self.new_jobs = []
        self.new_confirms = []

        if context:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.job_checking_thread = CheckJobs(self.job_bind_str,
                                             self.control_con_str,
                                             self.lock,
                                             log_queue,
                                             context)

#        self.conf_checking_thread = CheckConfirmations(self.conf_bind_str,
#                                                       self.lock,
#                                                       log_queue,
#                                                       context)

        self.create_sockets()

        try:
            self.run()
        except KeyboardInterrupt:
            pass

    def create_sockets(self):
        # socket to receive confirmation that data can be removed/discarded
        try:
            self.confirmation_socket = self.context.socket(zmq.PULL)

            self.confirmation_socket.bind(self.conf_bind_str)
            self.log.info("Start confirmation_socket (bind): '{0}'"
                          .format(self.conf_bind_str))
        except:
            self.log.error("Failed to start confirmation_socket (bind): '{0}'"
                           .format(self.conf_bind_str), exc_info=True)
            raise

        # socket for control signals
        try:
            self.control_socket = self.context.socket(zmq.SUB)
            self.control_socket.connect(self.control_con_str)
            self.log.info("Start control_socket (connect): '{0}'"
                          .format(self.control_con_str))
        except:
            self.log.error("Failed to start control_socket (connect): '{0}'"
                           .format(self.control_con_str), exc_info=True)
            raise

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")

        # register sockets at poller
        self.poller = zmq.Poller()
        self.poller.register(self.confirmation_socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)

    def run(self):
        global new_jobs
#        global new_confirmations

#        removable_elements = None
        old_confirmations = []

        self.job_checking_thread.start()
#        self.conf_checking_thread.start()

        while True:
            """
            # intersect
            removable_elements = new_jobs & new_confirmations
            self.log.debug("removable_elements={0}"
                           .format(removable_elements))

            for element in removable_elements:
                self.remove_element(element)

                new_jobs.discard(element)
                new_confirmations.discard(element)

            # do not loop too offen if there is nothing to process
            if not removable_elements:
                time.sleep(0.1)
            """

            socks = dict(self.poller.poll())

            ######################################
            #       messages from receiver       #
            ######################################
            if (self.confirmation_socket in socks
                    and socks[self.confirmation_socket] == zmq.POLLIN):

                self.log.debug("Waiting for confirmation")
                element = self.confirmation_socket.recv().decode("utf-8")
                self.log.debug("New confirmation received: {0}"
                               .format(element))
                self.log.debug("new_jobs={0}".format(new_jobs))
                self.log.debug("old_confirmations={0}"
                               .format(old_confirmations))

                for base_path, file_id in new_jobs:
                    if element == file_id:
                        self.remove_element(base_path, file_id)

                        self.lock.acquire()
                        new_jobs.remove([base_path, file_id])
                        self.lock.release()

                        self.log.debug("new_jobs={0}".format(new_jobs))
                    elif element in old_confirmations:
                        self.remove_element(element)

                        old_confirmations.remove(element)
                        self.log.debug("old_confirmations={0}"
                                       .format(old_confirmations))
                    else:
                        old_confirmations.append(element)
                        self.log.error("confirmations without job "
                                       "notification received: {0}"
                                       .format(element))

            ######################################
            #         control commands           #
            ######################################
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):
                try:
                    message = self.control_socket.recv_multipart()
                    self.log.debug("Control signal received")
                    self.log.debug("message = {0}".format(message))
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
                    self.log.error("Unhandled control signal received: {0}"
                                   .format(message))

    @abc.abstractmethod
    def remove_element(self, source_file_id):
        pass

    def stop(self):
        if self.job_checking_thread is not None:
            # give control signal time to arrive
            time.sleep(0.1)

            self.log.debug("Stopping job checking thread")
            self.job_checking_thread.stop()
            self.job_checking_thread.join()
            self.job_checking_thread = None

        if self.confirmation_socket is not None:
            self.log.debug("Closing confirmation socket")
            self.confirmation_socket.close(0)
            self.confirmation_socket = None

        if self.control_socket is not None:
            self.log.debug("Closing control socket")
            self.control_socket.close(0)
            self.control_socket = None

        if not self.ext_context and self.context is not None:
            self.log.debug("Destroying context")
            self.context.destroy(0)
            self.context = None

#        self.conf_checking_thread.stop()
#        self.conf_checking_thread.join()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    import logging
    import tempfile
    import shutil
    from multiprocessing import Queue, Process
    from logutils.queue import QueueHandler
    import socket

    """ Implement abstract class cleaner """
    class Cleaner(CleanerBase):
        def remove_element(self, source_file):
            # remove file
            try:
                os.remove(source_file)
                self.log.info("Removing file '{0}' ...success"
                              .format(source_file))
            except:
                self.log.error("Unable to remove file {0}".format(source_file),
                               exc_info=True)

    """ Set up logging """
    logfile = os.path.join(BASE_PATH, "logs", "cleaner.log")
    logsize = 10485760

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = utils.get_log_handlers(logfile, logsize, verbose=True,
                                      onscreen_log_level="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = utils.CustomQueueListener(log_queue, h1, h2)
    log_queue_listener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    """ Set up config """
    config = {
        "ipc_path": os.path.join(tempfile.gettempdir(), "hidra"),
        "current_pid": os.getpid(),
        "cleaner_port": 50051,
        "confirmation_port": 50052,
        "control_port": "50005"
    }

    con_ip = "zitpcx19282"
    ext_ip = socket.gethostbyaddr(con_ip)[2][0]

    context = zmq.Context.instance()

    # create ipc path
    if not os.path.exists(config["ipc_path"]):
        os.mkdir(config["ipc_path"])
        # the permission have to changed explicitly because
        # on some platform they are ignored when called within mkdir
        os.chmod(config["ipc_path"], 0o777)
        logging.info("Creating directory for IPC communication: {0}"
                     .format(config["ipc_path"]))

    """ determine socket connection strings """
    if utils.is_windows():
        job_con_str = "tcp://{0}:{1}".format(con_ip, config["cleaner_port"])
        job_bind_str = "tcp://{0}:{1}".format(ext_ip, config["cleaner_port"])

        control_con_str = "tcp://{0}:{1}".format(ext_ip,
                                                 config["control_port"])
    else:
        job_con_str = ("ipc://{0}/{1}_{2}".format(config["ipc_path"],
                                                  config["current_pid"],
                                                  "cleaner"))
        job_bind_str = job_con_str

        control_con_str = "ipc://{0}/{1}_{2}".format(config["ipc_path"],
                                                     config["current_pid"],
                                                     "control")

    conf_con_str = "tcp://{0}:{1}".format(con_ip, config["confirmation_port"])
    conf_bind_str = "tcp://{0}:{1}".format(ext_ip, config["confirmation_port"])

    """ Instantiate cleaner as additional process """
    cleaner_pr = Process(target=Cleaner,
                         args=(config,
                               log_queue,
                               job_bind_str,
                               conf_bind_str,
                               control_con_str,
                               context))
    cleaner_pr.start()

    """ Set up datafetcher simulator """
    job_socket = context.socket(zmq.PUSH)
    job_socket.connect(job_con_str)
    logging.info("=== Start job_socket (connect): {0}".format(job_con_str))

    """ Set up receiver simulator """
    confirmation_socket = context.socket(zmq.PUSH)
    confirmation_socket.connect(conf_con_str)
    logging.info("=== Start confirmation_socket (connect): {0}"
                 .format(conf_con_str))

    # to give init time to finish
    time.sleep(0.5)

    """ Test cleaner """
    source_file = os.path.join(BASE_PATH, "test_file.cbf")
    target_path = os.path.join(BASE_PATH, "data", "source", "local")

    try:
        for i in range(5):
            target_file = os.path.join(target_path, "{0}.cbf".format(i))
            shutil.copyfile(source_file, target_file)

            logging.debug("=== sending job {0}".format(target_file))
            job_socket.send_string(target_file)
            logging.debug("=== job sent {0}".format(target_file))

            confirmation_socket.send(target_file.encode("utf-8"))
            logging.debug("=== confirmation sent {0}".format(target_file))
    except KeyboardInterrupt:
        pass
    finally:
        time.sleep(1)
        cleaner_pr.terminate()
