# Copyright (C) DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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

"""Testing the event detector for synchronizing ewmscp events.
"""

# pylint: disable=missing-docstring

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import time
import zmq

import eventdetectors.experimental_events.sync_lambda_events as events
import hidra.utils as utils
from ..eventdetector_test_base import EventDetectorTestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestEventDetector(EventDetectorTestBase):
    """Specification of tests to be performed for the loaded EventDetector.
    """

    def setUp(self):
        super().setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        endpoint = ("ipc://{ipc_dir}/{pid}_internal_com"
                    .format(ipc_dir="/tmp/hidra", pid=1234))
        self.context = self.ed_base_config["context"]

        self.module_name = "sync_lambda_events"
        self.module_config = {
            "buffer_size": 50,
            # time to wait between image requests (in s)
            "wait_time": 1,
            "internal_com_endpoint": endpoint,
            # "device_names": ["haso228yy:10000/bltest/lambda/01"]
            # "device_names": ["haso111k:10000/petra3/lambda/01"]
            "device_names": ["haso228yy:10000/bltest/lambda/01",
                             "haso111k:10000/petra3/lambda/01"]
        }

        self.ed_base_config["config"]["eventdetector"] = {
            "type": self.module_name,
            self.module_name: self.module_config
        }

        self.eventdetector = None

    # ------------------------------------------------------------------------
    # Test general
    # ------------------------------------------------------------------------

    def test_general(self):

        self.eventdetector = events.EventDetector(self.ed_base_config)

        # pylint: disable=attribute-defined-outside-init
        self.internal_com_socket = self.start_socket(
            name="internal_com_socket",
            sock_type=zmq.PULL,
            sock_con="connect",
            endpoint=self.module_config["internal_com_endpoint"],
        )

        try:
            # get synchronized events
            for _ in range(2):
                event_list = self.eventdetector.get_new_event()
                for event in event_list:
                    recv_msg = self.internal_com_socket.recv_multipart()
                    data = [
                        utils.zmq_msg_to_nparray(
                            data=msg,
                            array_metadata=event["additional_info"][i]
                        )
                        for i, msg in enumerate(recv_msg)
                    ]
                    self.log.debug(data)
                time.sleep(self.module_config["wait_time"])
        finally:
            self.stop_socket(name="internal_com_socket")

    def tearDown(self):

        if self.eventdetector is not None:
            self.eventdetector.stop()
            self.eventdetector = None

        super().tearDown()
