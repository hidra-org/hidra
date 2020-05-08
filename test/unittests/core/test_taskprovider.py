# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""Testing the task provider.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import json
from multiprocessing import Process, freeze_support, Event
import os
import time
import threading
from shutil import copyfile
import socket
import sys
import zmq

try:
    import unittest.mock as mock
    import pathlib
except ImportError:
    # for python2
    import mock
    import pathlib2 as pathlib

from test_base import TestBase, create_dir, MockZmqSocket, MockLogging
from taskprovider import TaskProvider
import hidra.utils as utils

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
        self.log.info("request_fw_socket started (bind) for '%s'",
                      endpoint)

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
                self.log.debug("Received request: %s", request)

                message = json.dumps(open_requests).encode("utf-8")
                self.request_fw_socket.send(message)
                self.log.debug("Answer: %s", open_requests)
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

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()


class TestTaskProvider(TestBase):
    """Specification of tests to be performed for the TaskProvider.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super().setUp()

        # see https://docs.python.org/2/library/multiprocessing.html#windows
        freeze_support()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip
        # self.base_dir

        self.context = zmq.Context()

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        monitored_dir = os.path.join(self.base_dir, "data", "source")

        if sys.version_info[0] < 3:
            used_eventdetector = "inotifyx_events"
        else:
            used_eventdetector = "inotify_events"

        self.taskprovider_config = {
            "eventdetector": {
                "type": used_eventdetector,
                "inotify_events": {
                    "monitored_dir": monitored_dir,
                    "fix_subdirs": ["commissioning", "current", "local"],
                    "monitored_events": {
                        "IN_CLOSE_WRITE": [".tif", ".cbf"],
                        "IN_MOVED_TO": [".log"]
                    },
                    "event_timeout": 0.1,
                    "history_size": 0,
                    "use_cleanup": False,
                    "time_till_closed": 5,
                    "action_time": 120
                },
                "inotifyx_events": {
                    "monitored_dir": monitored_dir,
                    "fix_subdirs": ["commissioning", "current", "local"],
                    "monitored_events": {
                        "IN_CLOSE_WRITE": [".tif", ".cbf"],
                        "IN_MOVED_TO": [".log"]
                    },
                    "event_timeout": 0.1,
                    "history_size": 0,
                    "use_cleanup": False,
                    "time_till_closed": 5,
                    "action_time": 120
                }
            },
            "general": {
                "config_file": pathlib.Path("testnotconfig.yaml")
            }
        }

        self.start = 100
        self.stop = 105

    def test_taskprovider_terminate(self):
        """Simulate start up with wrong configuration.
        """
        stop_request = Event()

        endpoints = self.config["endpoints"]
        event_message_list = [{
            "filename": "test_file.cbf",
            "source_path": "my_source_path",
            "relative_path": "local"
        }]

        kwargs = dict(
            config=self.taskprovider_config,
            endpoints=endpoints,
            log_queue=self.log_queue,
            log_level="debug",
            stop_request=stop_request
        )

        mocked_fct = "{}.EventDetector.get_new_event".format(
            self.taskprovider_config["eventdetector"]["type"]
        )

        router_socket = self.start_socket(
            name="router_socket",
            sock_type=zmq.PULL,
            sock_con="connect",
            endpoint=endpoints.router_con
        )

        control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            # it is the sub endpoint because originally this is handled with
            # a zmq thread device
            endpoint=endpoints.control_sub_bind
        )

        with mock.patch(mocked_fct) as mock_get_events:
            mock_get_events.side_effect = [event_message_list]
            taskprovider_pr = Process(target=TaskProvider, kwargs=kwargs)
            taskprovider_pr.start()

        # wait till everything is set up
        time.sleep(0.5)

        self.log.info("Sending 'Exit' signal")
        control_socket.send_multipart([b"control", b"EXIT"])

        # give task provider time to react
        time.sleep(0.7)

        self.stop_socket(name="router_socket", socket=router_socket)
        self.stop_socket(name="control_socket", socket=control_socket)

    def test_taskprovider(self):
        """Simulate incoming data and check if received events are correct.
        """

        stop_request = Event()
        endpoints = self.config["endpoints"]

        kwargs = dict(
            config=self.taskprovider_config,
            endpoints=endpoints,
            log_queue=self.log_queue,
            log_level="debug",
            stop_request=stop_request
        )
        taskprovider_pr = Process(target=TaskProvider, kwargs=kwargs)
        taskprovider_pr.start()

        request_responder_pr = RequestResponder(self.config, self.log_queue)
        request_responder_pr.start()

        router_socket = self.start_socket(
            name="router_socket",
            sock_type=zmq.PULL,
            sock_con="connect",
            endpoint=endpoints.router_con
        )

        control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            # it is the sub endpoint because originally this is handled with
            # a zmq thread device
            endpoint=endpoints.control_sub_bind
        )

        source_file = os.path.join(self.base_dir,
                                   "test",
                                   "test_files",
                                   "test_file.cbf")
        target_file_base = os.path.join(self.base_dir,
                                        "data",
                                        "source",
                                        "local",
                                        "raw")

        create_dir(target_file_base)

        # give it time to start up
        time.sleep(0.5)

        try:
            for i in range(self.start, self.stop):
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(i))
                self.log.debug("copy to %s", target_file)
                copyfile(source_file, target_file)

                workload = router_socket.recv_multipart()
                self.log.info("next workload %s", workload)
        except KeyboardInterrupt:
            pass
        finally:
            self.log.info("send exit signal")
            control_socket.send_multipart([b"control", b"EXIT"])

            request_responder_pr.stop()

            self.stop_socket(name="router_socket", socket=router_socket)
            self.stop_socket(name="control_socket", socket=control_socket)

            for number in range(self.start, self.stop):
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(number))
                self.log.debug("remove %s", target_file)
                os.remove(target_file)

    def test_taskprovider_timeout(self):
        """Simulate incoming data and check if received events are correct.
        """

        stop_request = Event()
        endpoints = self.config["endpoints"]

        kwargs = dict(
            config=self.taskprovider_config,
            endpoints=endpoints,
            log_queue=self.log_queue,
            log_level="debug",
            stop_request=stop_request
        )
        # the method run contains setup and _run
        with mock.patch("taskprovider.TaskProvider.run"):
            taskprovider = TaskProvider(**kwargs)

        taskprovider.log = MockLogging()
        taskprovider.log.error = mock.MagicMock()
        taskprovider.eventdetector = mock.MagicMock()
        taskprovider._check_control_socket = mock.MagicMock()

        taskprovider.context = self.context
        taskprovider.timeout = 1000
        taskprovider.create_sockets()

        # properly stop the not needed sockets and mock them
        taskprovider.stop_socket(name="router_socket")
        taskprovider.stop_socket(name="control_socket")
        taskprovider.router_socket = MockZmqSocket()
        # to avoid Exception AssertionError when stopping the taskprovider
        taskprovider.router_socket.connect("test")
        taskprovider.control_socket = MockZmqSocket()
        # to avoid Exception AssertionError when stopping the taskprovider
        taskprovider.control_socket.connect("test")

        # first iteration: go on
        # second iteration: stop taskprovider
        taskprovider._check_control_socket.side_effect = [False, True]

        taskprovider.eventdetector.get_new_event.return_value = [
            {
                "filename": "1.tif",
                "source_path": "/my_dir",
                "relative_path": "local"
            }
        ]

        # since there is no REP socket, this runs into the timeout
        taskprovider._run()

        if taskprovider.log.error.called:
            self.log.debug(taskprovider.log.error.call_args[0][0])
            self.assertNotIn("failed", taskprovider.log.error.call_args[0][0])

    def tearDown(self):
        self.context.destroy(0)

        super().tearDown()
