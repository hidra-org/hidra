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

import time
import zmq

from test_base import TestBase
from statserver import StatServer
import multiprocessing

import hidra.utils as utils


class TestStatServer(TestBase):
    """Specification of tests to be performed for the StatsServer.
    """

    def setUp(self):
        super(TestStatServer, self).setUp()

        control_endpt = "ipc:///tmp/control"
        stats_collect_endpt = "ipc:///tmp/stats_collect"
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
            com_con=None,
            cleaner_job_bind=None,
            cleaner_job_con=None,
            cleaner_trigger_bind=None,
            cleaner_trigger_con=None,
            confirm_bind=None,
            confirm_con=None,
            stats_collect_bind=stats_collect_endpt,
            stats_collect_con=stats_collect_endpt,
            stats_expose_bind=None,
            stats_expose_con=None,
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

        #self.server = StatServer(self.statserver_config, self.log_queue)
        self.server = multiprocessing.Process(
            target=StatServer,
            args = (self.statserver_config, self.log_queue)
        )
        self.server.start()
        time.sleep(0.1)

    def test_statsserver(self):
        pass

    def tearDown(self):
        if self.control_socket is not None:
            self.log.info("Sending 'Exit' signal")
            self.control_socket.send_multipart([b"control", b"EXIT"])

        time.sleep(0.1)

        super(TestStatServer, self).tearDown()