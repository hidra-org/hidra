# Copyright (C) 2019  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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
import multiprocessing
import time
import zmq

from test_base import TestBase
from statserver import run_statserver  # , StatServer

import hidra.utils as utils


class TestStatServer(TestBase):
    """Specification of tests to be performed for the StatsServer.
    """

    def setUp(self):
        super().setUp()

        control_endpt = "ipc:///tmp/control"
        stats_collect_endpt = "ipc:///tmp/stats_collect"
        stats_expose_endpt = "ipc:///tmp/stats_expose"
        com_con = "tcp://abc:123"
        self.endpoints = utils.Endpoints(
            control_pub_bind=None,
            control_pub_con=None,
            control_sub_bind=control_endpt,
            control_sub_con=control_endpt,
            request_bind=None,
            request_con=None,
            request_fw_bind=None,
            request_fw_con=None,
            router_bind=None,
            router_con=None,
            com_bind=None,
            com_con=com_con,
            cleaner_job_bind=None,
            cleaner_job_con=None,
            cleaner_trigger_bind=None,
            cleaner_trigger_con=None,
            confirm_bind=None,
            confirm_con=None,
            stats_collect_bind=stats_collect_endpt,
            stats_collect_con=stats_collect_endpt,
            stats_expose_bind=stats_expose_endpt,
            stats_expose_con=stats_expose_endpt,
        )

        self.statserver_config = {
            "network": {
                "endpoints": self.endpoints
            }
        }

        self.context = zmq.Context()

        # socket for control signals
        self.control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            endpoint=self.endpoints.control_sub_bind
        )

        stop_request = multiprocessing.Event()

        self.server = multiprocessing.Process(
            target=run_statserver,
            kwargs=dict(
                config=self.statserver_config,
                log_queue=self.log_queue,
                log_level="debug",
                stop_request=stop_request
            )
        )
        self.server.start()
        time.sleep(0.1)

    def test_statserver_exposing(self):
        """functional tests of the stat server.
        """

        stats_expose_socket = self.start_socket(
            name="stats_expose_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint=self.endpoints.stats_expose_con
        )

        key = json.dumps("config").encode()
        stats_expose_socket.send(key)

        answer = stats_expose_socket.recv()
        answer = json.loads(answer.decode())
        self.log.debug("answer=%s", answer)

        endpt = utils.Endpoints(*answer["network"]["endpoints"])
        self.log.debug("com_con=%s", endpt.com_con)

    def tearDown(self):
        if self.control_socket is not None:
            self.log.info("Sending 'Exit' signal")
            self.control_socket.send_multipart([b"control", b"EXIT"])

        time.sleep(0.1)

        super().tearDown()
