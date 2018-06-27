"""Testing the task provider.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import time
import threading
import socket
import zmq
from shutil import copyfile
from multiprocessing import Process, freeze_support

from .__init__ import BASE_DIR
from test_base import TestBase, create_dir
from taskprovider import TaskProvider
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class RequestResponder(threading.Thread):
    """A signal handler simulator to answer requests.
    """

    def __init__(self, config, log_queue):
        threading.Thread.__init__(self)

        self.config = config
        self.continue_run = True

        # Send all logs to the main process
        self.log = utils.get_logger("RequestResponder", log_queue)

        self.context = zmq.Context()

        endpoint = self.config["endpoints"].request_fw_con
        self.request_fw_socket = self.context.socket(zmq.REP)
        self.request_fw_socket.bind(endpoint)
        self.log.info("request_fw_socket started (bind) for '{}'"
                      .format(endpoint))

    def run(self):
        """Answer to all incoming requests.
        """

        hostname = socket.getfqdn()
        self.log.info("Start run")

        open_requests = [
            ['{}:6003'.format(hostname), 1, [".cbf"]],
            ['{}:6004'.format(hostname), 0, [".cbf"]]
        ]

        while self.continue_run:
            try:
                request = self.request_fw_socket.recv_multipart()
                self.log.debug("Received request: {}".format(request))

                message = json.dumps(open_requests).encode("utf-8")
                self.request_fw_socket.send(message)
                self.log.debug("Answer: {}".format(open_requests))
            except zmq.ContextTerminated:
                self.log.debug("ContextTerminated -> break")
                break

    def stop(self):
        """Clean up.
        """
        if self.continue_run:
            self.continue_run = False

            self.request_fw_socket.close(0)
            self.context.term()

    def __exit__(self):
        self.stop()


class TestTaskProvider(TestBase):
    """Specification of tests to be performed for the TaskProvider.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestTaskProvider, self).setUp()

        # see https://docs.python.org/2/library/multiprocessing.html#windows
        freeze_support()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        self.context = zmq.Context()

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self.taskprovider_config = {
            "event_detector_type": "inotifyx_events",
            "monitored_dir": os.path.join(BASE_DIR, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {
                "IN_CLOSE_WRITE": [".tif", ".cbf"],
                "IN_MOVED_TO": [".log"]
            },
            "timeout": 0.1,
            "history_size": 0,
            "use_cleanup": False,
            "time_till_closed": 5,
            "action_time": 120
        }

        self.start = 100
        self.stop = 105

    def test_taskprovider(self):
        """Simulate incoming data and check if received events are correct.
        """

        endpoints = self.config["endpoints"]

        kwargs = dict(
            config=self.taskprovider_config,
            endpoints=endpoints,
            log_queue=self.log_queue
        )
        taskprovider_pr = Process(target=TaskProvider, kwargs=kwargs)
        taskprovider_pr.start()

        request_responder_pr = RequestResponder(self.config, self.log_queue)
        request_responder_pr.start()

        router_socket = self.context.socket(zmq.PULL)
        router_socket.connect(endpoints.router_con)
        self.log.info("router_socket connected to {}"
                      .format(endpoints.router_con))

        source_file = os.path.join(BASE_DIR, "test_file.cbf")
        target_file_base = os.path.join(BASE_DIR,
                                        "data",
                                        "source",
                                        "local",
                                        "raw")

        create_dir(target_file_base)

    #    time.sleep(5)
        try:
            for i in range(self.start, self.stop):
                time.sleep(0.5)
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(i))
                self.log.debug("copy to {}".format(target_file))
                copyfile(source_file, target_file)

                workload = router_socket.recv_multipart()
                self.log.info("next workload {}".format(workload))
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:

            request_responder_pr.stop()
            taskprovider_pr.terminate()

            router_socket.close(0)

            for number in range(self.start, self.stop):
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(number))
                self.log.debug("remove {}".format(target_file))
                os.remove(target_file)

    def tearDown(self):
        self.context.destroy(0)

        super(TestTaskProvider, self).tearDown()
