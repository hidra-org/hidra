# Copyright (C) 2020  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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

"""
This module implements the ASAP::O data forwarding

Needed configuration in config_file:
datareceiver:
    plugin: "asapo_producer"

asapo_producer:
    endpoint: string
    beamtime: string
    stream: string
    token: string
    n_threads: int
    ingest_mode: string

Example config:
    asapo_producer:
        endpoint: "asapo-services:8400"
        beamtime: "asapo_test"
        stream: "hidra_test"
        token: "KmUDdacgBzaOD3NIJvN1NmKGqWKtx0DK-NyPjdpeWkc="
        n_threads: 1
        ingest_mode: INGEST_MODE_TRANSFER_METADATA_ONLY
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import asapo_producer
import asapo_consumer
from builtins import super  # pylint: disable=redefined-builtin
import json
import logging
import threading

try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path


class Plugin(object):
    """Implements an ASAP::O producer plugin
    """
    def __init__(self, plugin_config):
        super().__init__()

        self.config = plugin_config

        self.producer = None
        self.endpoint = None
        self.beamtime = None
        self.token = None
        self.stream = None
        self.ingest_mode = None
        self.data_type = None
        self.file_id = None
        self.lock = None

        self.log = None

    def setup(self):
        """Sets the configuration and starts the producer
        """
        self.log = logging.getLogger()

        self.endpoint = self.config["endpoint"]
        self.beamtime = self.config["beamtime"]
        self.stream = self.config["stream"]
        self.token = self.config["token"]
        n_threads = self.config["n_threads"]
        self._set_ingest_mode(self.config["ingest_mode"])

        self.lock = threading.Lock()

        self.producer = asapo_producer.create_producer(
            endpoint=self.endpoint,
            beamtime_id=self.beamtime,
            stream=self.stream,
            token=self.token,
            nthreads=n_threads
        )
        self.file_id = self._get_start_file_id()

    def _set_ingest_mode(self, mode):
        if mode == "INGEST_MODE_TRANSFER_METADATA_ONLY":
            self.ingest_mode = asapo_producer.INGEST_MODE_TRANSFER_METADATA_ONLY
            self.data_type = "metadata"
        else:
            raise NotSupported("Ingest mode '{}' is not supported".format(mode))

    def _get_start_file_id(self):
        path = "/asapo_shared/asapo/data"

        broker = asapo_consumer.create_server_broker(
            server_name=self.endpoint,
            source_path=path,
            beamtime_id=self.beamtime,
            stream=self.stream,
            token=self.token,
            timeout_ms=1000
        )
        group_id = broker.generate_group_id()
        data, metadata = broker.get_last(group_id, meta_only=True)

        return metadata["_id"]

        # TODO if empty set to 0
        # last_id = 1

    def get_data_type(self):
        return self.data_type

    def process(self, local_path, metadata, data=None):
        """Send the file to the ASAP::O producer

        Args:
            local_path: The absolute path where the file was written
            metadata: The metadata to send as dict
            data (optional): the data to send
        """
        self.file_id += 1
        exposed_path = Path(metadata["relative_path"],
                            metadata["filename"]).as_posix()

        self.producer.send_file(
            id=self.file_id,
            local_path=local_path,
            exposed_path=exposed_path,  # self.stream+"/test2_file",
            ingest_mode=self.ingest_mode,
            user_meta=json.dumps({"hidra": metadata}),
            callback=self._callback
        )

    def _callback(self, header, err):
        self.lock.acquire()
        if err is None:
            self.log.debug("successfully sent: %s", header)
        else:
            self.log.error("could not sent: %s, %s", header, err)
        self.lock.release()

    def aaa_stop(self):
        pass

    def stop(self):
        self.producer.wait_requests_finished(2000)
