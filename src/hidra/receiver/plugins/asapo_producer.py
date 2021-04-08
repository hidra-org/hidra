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
    beamline: string
    beamtime: string # optional
    data_source: string  # optional
    token: string  # optional
    token_file: string  # needed if token is not static
    n_threads: int
    ingest_mode: string
    file_regex: regex string
    ignore_regex: regex string
    files_in_scan_start_index: int  # optional
    sequential_idx: bool # file index is generated

Example config:
    asapo_producer:
        endpoint: "asapo-services:8400"
        beamline: "my_beamline"
        beamtime: "asapo_test"
        data_source: "hidra_test"
        token: "KmUDdacgBzaOD3NIJvN1NmKGqWKtx0DK-NyPjdpeWkc="
        n_threads: 1
        ingest_mode: INGEST_MODE_TRANSFER_METADATA_ONLY
        file_regex: '.*/(?P<data_source>.*)/scan_(?P<scan_id>.*)/'
                    '(?P<file_idx_in_scan>.*).tif'
        ignore_regex: '.*/.*.metadata$'
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from builtins import super  # pylint: disable=redefined-builtin
import json
import logging
from os import path
import re
import threading
from time import time

from future.utils import iteritems

import asapo_producer
import hidra.utils as utils

try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path


class Ignored(Exception):
    """Raised when an event is processed that should be ignored."""
    pass


class Plugin(object):
    """Implements an ASAP::O producer plugin
    """

    def __init__(self, plugin_config):
        super().__init__()

        self.config = plugin_config
        self.required_parameter = []

        self.timeout = 1000
        self.config_time = 0
        self.check_time = 0

        self.asapo_worker = None
        self.data_type = None
        self.lock = None
        self.log = None

    def setup(self):
        """Sets the configuration and starts the producer
        """
        self.log = logging.getLogger(__name__)
        self.required_parameter = [
            "endpoint",
            "n_threads",
            "ingest_mode",
            "user_config_path",
            "file_regex",
            "beamline"
        ]
        self._check_config()
        self._set_ingest_mode(self.config["ingest_mode"])

        if "data_source" in self.config:
            self.log.debug("Static data_source configured. Using: %s", self.data_source)

        if "token" in self.config:
            self.log.debug("Static token configured.")

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
            ingest_mode = (asapo_producer
                           .INGEST_MODE_TRANSFER_METADATA_ONLY)
            self.data_type = "metadata"
        # elif mode == "INGEST_MODE_TRANSFER_DATA":
        #    self.ingest_mode = asapo_producer.INGEST_MODE_TRANSFER_DATA
        #    self.data_type = "metadata"
        elif mode == "DEFAULT_INGEST_MODE":
            ingest_mode = asapo_producer.DEFAULT_INGEST_MODE
            self.data_type = "metadata"
        else:
            raise utils.NotSupported("Ingest mode '{}' is not supported"
                                     .format(mode))
        self.config['ingest_mode'] = ingest_mode

    def get_data_type(self):
        return self.data_type

    def process(self, local_path, metadata, data=None):
        """Send the file to the ASAP::O producer

        Asapo data_source and index are chosen by the incoming files in format
        (<scan_id, <file_id>) -> (stream, id)

        Args:
            local_path: The absolute path where the file was written
            metadata: The metadata to send as dict
            data (optional): the data to send
        """

        if self._config_is_modified() or self.asapo_worker is None:
            self.asapo_worker = AsapoWorker(self.config)

        self.asapo_worker.send_message(local_path, metadata)

    def _config_is_modified(self):
        ts = time()
        if (self.check_time - ts) > self.timeout:
            self.check_time = ts
            file_path = self.config["user_config_path"]
            if self.config_time != path.getmtime(file_path):
                self.config_time = path.getmtime(file_path)
                return True
        return False

    def stop(self):
        """ Clean up """
        self.asapo_worker.stop()


class AsapoWorker:
    def __init__(self, config):

        self.config = config
        user_config = utils.load_config(config["user_config_path"])
        self.config.update(user_config)

        self.endpoint = self.config["endpoint"]
        self.n_threads = self.config["n_threads"]
        self.timeout = self.config["timeout"]
        self.beamtime = self.config["beamtime"]
        self.token = self.config["token"]
        self.ingest_mode = self.config["ingest_mode"]

        self.data_source = self.config.get("data_source", None)
        self.file_regex = self.config["file_regex"]

        if "ignore_regex" in self.config:
            self.ignore_regex = self.config["ignore_regex"]
            self.log.debug("Ignoring files matching '%s'.", self.ignore_regex)

        self.lock = threading.Lock()
        self.log = logging.getLogger(__name__)
        self.data_source_info = {}

    def _create_producer(self, data_source):

        config = dict(
            endpoint=self.endpoint,
            type="raw",
            beamtime_id=self.beamtime,
            beamline="auto",
            data_source=data_source,
            token=self.token,
            nthreads=self.n_threads,
            timeout_ms=self.timeout * 1000
        )

        self.log.info("Create producer with config=%s", config)
        self.data_source_info[data_source] = {
            "producer": asapo_producer.create_producer(**config),
        }

    def send_message(self, local_path, metadata):

        try:
            data_source, stream, file_idx = self._parse_file_name(local_path)
        except Ignored:
            self.log.debug("Ignoring file %s", local_path)
            return

        if data_source not in self.data_source_info:
            self._create_producer(data_source=data_source)

        producer = self.data_source_info[data_source]["producer"]

        data = None
        if self.ingest_mode != asapo_producer.INGEST_MODE_TRANSFER_METADATA_ONLY:
            data = self._get_data(local_path)

        self.log.debug("using stream %s", stream)
        producer.send(id=file_idx + 1,  # files start with index 0 and asapo with 1
                      exposed_path=self._get_exposed_path(metadata),
                      data=data,
                      user_meta=json.dumps({"hidra": metadata}),
                      ingest_mode=self.ingest_mode,
                      stream=stream,
                      callback=self._callback)

    @staticmethod
    def _get_data(self, local_path):
        with open(str(local_path), "rb") as f:
            return f.read()

    def _callback(self, header, err):
        self.lock.acquire()
        header = {key: val for key, val in header.items() if key != 'data'}
        if err is None:
            self.log.debug("Successfully sent: %s", header)
        else:
            self.log.error("Could not sent: %s, %s", header, err)
        self.lock.release()

    @staticmethod
    def _get_exposed_path(metadata):
        exposed_path = Path(metadata["relative_path"],
                            metadata["filename"]).parts

        # TODO this is a workaround
        # asapo work on the core fs only at the moment thus the current
        # directory does not exist there
        if exposed_path[0] == "current":
            exposed_path = Path().joinpath(*exposed_path[1:]).as_posix()
        else:
            raise utils.NotSupported(
                "Path '{}' is not supported"
                    .format(Path().joinpath(*exposed_path).as_posix())
            )

        return exposed_path

    def _parse_file_name(self, path):
        # check for ignored files
        if re.search(self.ignore_regex, path):
            raise Ignored("Ignoring file {}".format(path))

        # parse file name
        search = re.search(self.file_regex, path)
        if search:
            matched = search.groupdict()
        else:
            self.log.debug("file name: %s", path)
            self.log.debug("file_regex: %s", self.file_regex)
            raise utils.UsageError("Does not match file pattern")

        if self.data_source is not None:
            data_source = self.data_source
        else:
            try:
                data_source = matched["data_source"]
            except KeyError:
                raise utils.UsageError("Missing entry for data_source in matched "
                                       "result")

        try:
            stream = matched["scan_id"]
        except KeyError:
            raise utils.UsageError("Missing entry for scan_id in matched "
                                   "result")

        try:
            file_idx = int(matched["file_idx_in_scan"])
        except KeyError:
            raise utils.UsageError("Missing entry for file_idx_in_scan in "
                                   "matched result")

        return data_source, stream, file_idx

    def stop(self):
        for _, info in iteritems(self.data_source_info):
            info["producer"].wait_requests_finished(2000)
