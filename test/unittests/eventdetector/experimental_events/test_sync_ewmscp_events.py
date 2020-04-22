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

import json
import threading

from kafka import KafkaProducer
import eventdetectors.experimental_events.sync_ewmscp_events as events
from ..eventdetector_test_base import EventDetectorTestBase

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class LambdaSimulator(threading.Thread):

    def __init__(self, server, topic, detid, n_files):
        super().__init__()

        self.topic = topic
        self.detid = detid
        self.n_files = n_files

        self.producer = KafkaProducer(
            bootstrap_servers=server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def run(self):

        filename = "{}_{:03}.h5"

        message = {
            "finishTime": 1556031799.7914205,
            "inotifyTime": 1556031799.791173,
            "md5sum": "",
            "operation": "copy",
            "path": "/my_dir/my_subdir/",
            "retries": 1,
            "size": 43008,
            "source": "./my_subdir/"
        }

        for i in range(self.n_files):
            message["path"] += filename.format(self.detid, i)
            message["source"] += filename.format(self.detid, i)
            future = self.producer.send(self.topic, message)
            future.get(timeout=60)


class TestEventDetector(EventDetectorTestBase):
    """Specification of tests to be performed for the loaded EventDetector.
    """

    def setUp(self):
        super().setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        self.module_name = "sync_ewmscp_events"
        self.module_config = {
            "buffer_size": 50,
            "monitored_dir": "/my_dir",
            "fix_subdirs": ["my_subdir"],
            "kafka_server": ["asap3-events-01", "asap3-events-02"],
            "kafka_topic": "kuhnm_test",
            "operation": "copy",
            "detids": ["DET0", "DET1", "DET2"],
            "n_detectors": 3
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
        # pylint: disable=unused-argument

        self.eventdetector = events.EventDetector(self.ed_base_config)

        server = self.module_config["kafka_server"]
        topic = self.module_config["kafka_topic"]
        n_det = self.module_config["n_detectors"]
        n_files = 1

        # produce kafka data
        dets = []
        for i in range(n_det):
            dets.append(
                LambdaSimulator(server, topic, "DET{}".format(i), n_files)
            )
        for i in dets:
            i.start()

        # get synchronized events
        for i in range(3):
            self.log.debug("run")
            event_list = self.eventdetector.get_new_event()
            self.log.debug("event_list: %s", event_list)

        # shut down producer
        for i in dets:
            i.join()

    def tearDown(self):

        if self.eventdetector is not None:
            self.eventdetector.stop()
            self.eventdetector = None

        super().tearDown()
