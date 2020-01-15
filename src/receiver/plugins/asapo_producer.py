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
from future.utils import iteritems
import hidra.utils as utils
import json
import logging
import re
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
        self.required_parameter = []

        self.stream_info = {}
        self.endpoint = None
        self.beamtime = None
        self.token = None
        self.stream = None
        self.n_threads = None
        self.ingest_mode = None
        self.data_type = None
        self.lock = None

        self.log = None

    def setup(self):
        """Sets the configuration and starts the producer
        """
        self.log = logging.getLogger(__name__)

        self.required_parameter = [
            "endpoint",
            "beamtime",
            "token",
            "n_threads",
            "ingest_mode"
        ]
        self._check_config()

        self.endpoint = self.config["endpoint"]
        self.beamtime = self.config["beamtime"]
        self.token = self.config["token"]
        self.n_threads = self.config["n_threads"]
        self._set_ingest_mode(self.config["ingest_mode"])

        try:
            self.stream = self.config["stream"]
            self.log.debug("Static stream configured. Using: %s", self.stream)
        except KeyError:
            pass

        self.lock = threading.Lock()

    def _check_config(self):
        failed = False

        for i in self.required_parameter:
            if i not in self.config:
                self.log.error(
                    "Wrong configuration. Missing parameter: '%s'", i
                )
                failed = True

        if failed:
            raise utils.WrongConfiguration(
                "The configuration has missing or wrong parameters."
            )

    def _set_ingest_mode(self, mode):
        if mode == "INGEST_MODE_TRANSFER_METADATA_ONLY":
            self.ingest_mode = asapo_producer.INGEST_MODE_TRANSFER_METADATA_ONLY
            self.data_type = "metadata"
        # TODO data forwarding
#        elif mode == "INGEST_MODE_TRANSFER_DATA":
#            self.ingest_mode = asapo_producer.INGEST_MODE_TRANSFER_DATA
#            self.data_type = "data"
#        elif mode == "DEFAULT_INGEST_MODE":
#            self.ingest_mode = asapo_producer.DEFAULT_INGEST_DATA
#            self.data_type = "data"
        else:
            raise utils.NotSupported("Ingest mode '{}' is not supported".format(mode))

    def _get_start_file_id(self, stream):
        consumer_config = dict(
            server_name=self.endpoint,
            source_path="",
            beamtime_id=self.beamtime,
            stream=stream,
            token=self.token,
            timeout_ms=1000
        )
        broker = asapo_consumer.create_server_broker(**consumer_config)
        group_id = broker.generate_group_id()
        try:
            data, metadata = broker.get_last(group_id, meta_only=True)
            file_id = metadata["_id"]
            self.log.debug("Continue existing stream (id %s)", file_id)
            return file_id
        except (asapo_consumer.AsapoWrongInputError,
                asapo_consumer.AsapoEndOfStreamError):
            self.log.debug("Starting new stream (id 1)")
            return 0
        except Exception:
            self.log.debug("Config for consumer was: %s", consumer_config)
            raise

    def get_data_type(self):
        return self.data_type

    def _create_producer(self, stream):
        config = dict(
            endpoint=self.endpoint,
            beamtime_id=self.beamtime,
            stream=stream,
            token=self.token,
            nthreads=self.n_threads
        )
        self.log.debug("Create producer with config=%s", config)
        self.stream_info[stream] = {
            "producer": asapo_producer.create_producer(**config),
            "offset": self._get_start_file_id(stream),
            "last_id_used": 0,
            "current_scan_id": 0
        }

    def process(self, local_path, metadata, data=None):
        """Send the file to the ASAP::O producer

        Asapo index calculation explained for incoming files in format
        (<scan_id, <file_id>):
        (0,0) -> 1
        (0,1) -> 2
        (1,0) -> 3
        (0,3) -> dropped (old scan)
        (1,0) -> 3, but asapo is immutable -> dropped
        (1,1) -> 4

        Args:
            local_path: The absolute path where the file was written
            metadata: The metadata to send as dict
            data (optional): the data to send
        """
        exposed_path = Path(metadata["relative_path"],
                            metadata["filename"]).as_posix()

        detector, scan_id, file_id = self._parse_file_name(local_path)

        stream = self.stream or detector

        if stream not in self.stream_info:
            self._create_producer(stream=stream)

        # for convenience
        stream_info = self.stream_info[stream]
        producer = stream_info["producer"]

        # drop file from old scan to not mess up data of new scan
        if scan_id < stream_info["current_scan_id"]:
            self.log.debug("current_scan_id=%s, scan_id=%s",
                           stream_info["current_scan_id"], scan_id)
            self.log.debug("local_path=%s", local_path)
            raise utils.DataError("File belongs to old scan id. Drop it.")
        # new scan means file_id counting is reset -> offset has to adjusted
        elif scan_id > stream_info["current_scan_id"]:
            self.log.debug("Detected new scan, increase offset by %s",
                           stream_info["last_id_used"])
            # increase by the sum of all files of previous scan
            stream_info["offset"] += stream_info["last_id_used"]

        asapo_id = file_id + stream_info["offset"]

        if self.data_type == "metadata":
            producer.send_file(
                id=asapo_id,
                local_path=local_path,
                exposed_path=exposed_path,
                ingest_mode=self.ingest_mode,
                user_meta=json.dumps({"hidra": metadata}),
                callback=self._callback
            )
        elif self.data_type == "data":
            producer.send_data(
                id=asapo_id,
                exposed_path=exposed_path,
                data=data,
                user_meta=json.dumps({"hidra": metadata}),
                ingest_mode=self.ingest_mode,
                callback=self._callback
            )
        else:
            raise utils.NotSupported("No correct data_type was specified. "
                                     "Was setup method executed?")

        stream_info["last_id_used"] = file_id
        stream_info["current_scan_id"] = scan_id

    def _parse_file_name(self, path):
        regex = self.config["file_regex"]

        search = re.search(regex, path)
        if search:
            matched = search.groupdict()
        else:
            self.log.debug("file name: %s", path)
            self.log.debug("regex: %s", regex)
            raise utils.UsageError("Does not match file pattern")

        try:
            detector = matched["detector"]
        except KeyError:
            raise utils.UsageError("Missing entry for detector in matched "
                                   "result")
        try:
            scan_id = matched["scan_id"]
        except KeyError:
            raise utils.UsageError("Missing entry for scan_id in matched "
                                   "result")
        try:
            file_id = matched["file_idx_in_scan"]
        except KeyError:
            raise utils.UsageError("Missing entry for file_idx_in_scan in "
                                   "matched result")

        return detector, int(scan_id), int(file_id)

    def _callback(self, header, err):
        self.lock.acquire()
        if err is None:
            self.log.debug("Successfully sent: %s", header)
        else:
            self.log.error("Could not sent: %s, %s", header, err)
        self.lock.release()

    def stop(self):
        for stream, info in iteritems(self.stream_info):
            info["producer"].wait_requests_finished(2000)
