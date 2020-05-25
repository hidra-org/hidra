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
    beamtime_file_path: string  # needed if beamtime is not static
    beamtime_file_regex: string  # needed if beamtime is not static
    stream: string  # optional
    token: string  # optional
    token_file: string  # needed if token is not static
    n_threads: int
    ingest_mode: string
    file_regex: regex string
    ignore_regex: regex string
    files_in_scan_start_index: int  # optional

Example config:
    asapo_producer:
        endpoint: "asapo-services:8400"
        beamline: "my_beamline"
        beamtime: "asapo_test"
        stream: "hidra_test"
        token: "KmUDdacgBzaOD3NIJvN1NmKGqWKtx0DK-NyPjdpeWkc="
        n_threads: 1
        ingest_mode: INGEST_MODE_TRANSFER_METADATA_ONLY
        file_regex: '.*/(?P<stream>.*)/scan_(?P<scan_id>.*)/'
                    '(?P<file_idx_in_scan>.*).tif'
        ignore_regex: '.*/.*.metadata$'
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
import os
import re
import threading

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

        self.stream_info = {}

        self.endpoint = None
        self.beamtime = None
        self.beamtime_file_path = None
        self.beamtime_file_regex = None
        self.token = None
        self.token_file = None
        self.stream = None
        self.n_threads = None
        self.ingest_mode = None
        self.file_regex = None
        self.ignore_regex = None
        self.file_start_index = None

        self.timeout = 1000

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
            "file_regex",
            "beamline"
        ]
        self._check_config()

        self.endpoint = self.config["endpoint"]
        self.n_threads = self.config["n_threads"]
        self._set_ingest_mode(self.config["ingest_mode"])
        self.file_regex = self.config["file_regex"]
        self.beamline = self.config["beamline"]

        try:
            self.beamtime = self.config["beamtime"]
        except KeyError:
            try:
                self.beamtime_file_path = self.config["beamtime_file_path"]
                self.beamtime_file_regex = self.config["beamtime_file_regex"]
            except KeyError:
                raise utils.WrongConfiguration(
                    "Missing token specification. Either configure a static "
                    "token or the path to the token file."
                )

        try:
            self.stream = self.config["stream"]
            self.log.debug("Static stream configured. Using: %s", self.stream)
        except KeyError:
            pass

        try:
            self.token = self.config["token"]
            self.log.debug("Static token configured.")
        except KeyError:
            try:
                self.token_file = self.config["token_file"]
            except KeyError:
                raise utils.WrongConfiguration(
                    "Missing token specification. Either configure a static "
                    "token or the path to the token file."
                )

        try:
            self.ignore_regex = self.config["ignore_regex"]
            self.log.debug("Ignoring files matching '%s'.", self.ignore_regex)
        except KeyError:
            pass

        try:
            self.file_start_index = self.config["files_in_scan_start_index"]
        except KeyError:
            self.file_start_index = 0

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
            self.ingest_mode = (asapo_producer
                                .INGEST_MODE_TRANSFER_METADATA_ONLY)
            self.data_type = "metadata"
        # TODO data forwarding
#        elif mode == "INGEST_MODE_TRANSFER_DATA":
#            self.ingest_mode = asapo_producer.INGEST_MODE_TRANSFER_DATA
#            self.data_type = "data"
#        elif mode == "DEFAULT_INGEST_MODE":
#            self.ingest_mode = asapo_producer.DEFAULT_INGEST_DATA
#            self.data_type = "data"
        else:
            raise utils.NotSupported("Ingest mode '{}' is not supported"
                                     .format(mode))

    def get_data_type(self):
        return self.data_type

    def _create_producer(self, stream):
        token = self.token or self._get_token()
        beamtime = self.beamtime or self._get_beamtime()
        self.log.debug("type of beamtime%s", type(beamtime))

        config = dict(
            endpoint=self.endpoint,
            beamtime_id=beamtime,
            beamline=self.beamline,
            stream=stream,
            token=token,
            nthreads=self.n_threads,
            timeout_sec=self.timeout        )

        self.log.info("Create producer with config=%s", config)

        self.stream_info[stream] = {
            "producer": asapo_producer.create_producer(**config),
        }

    def _get_token(self):
        with open(self.token_file, "r") as f:
            token = f.read().replace('\n', '')

        return token

    def _get_beamtime(self):
        (_, _, filenames) = next(os.walk(self.beamtime_file_path))
        self.log.debug("filesnames = %s", filenames)
        for name in filenames:
            search = re.search(self.beamtime_file_regex, name)
            if search:
                matched = search.groupdict()

                try:
                    beamtime = matched["beamtime"]
                    self.log.debug("Using beamtime %s", beamtime)
                    return beamtime
                except KeyError:
                    raise utils.UsageError("Missing entry for beamtime in "
                                           "matched result")

        self.log.debug("beamtime_file_path=%s", self.beamtime_file_path)
        self.log.debug("beamtime_file_regex=%s", self.beamtime_file_regex)

        raise utils.WrongConfiguration("No matching beamtime metadata file "
                                       "found.")

    def process(self, local_path, metadata, data=None):
        """Send the file to the ASAP::O producer

        Asapo stream and index are chosen by the incoming files in format
        (<scan_id, <file_id>) -> (substream, id)

        Args:
            local_path: The absolute path where the file was written
            metadata: The metadata to send as dict
            data (optional): the data to send
        """
        try:
            stream_id, scan_id, file_id = self._parse_file_name(local_path)
        except Ignored:
            self.log.debug("Ignoring file %s", local_path)
            return

        stream = self.stream or stream_id

        if stream not in self.stream_info:
            self._create_producer(stream=stream)

        # for convenience
        stream_info = self.stream_info[stream]
        producer = stream_info["producer"]

        substream = str(scan_id)
        self.log.debug("using substream %s", substream)

        config = dict(
            id=file_id + 1,  # files start with index 0 and asapo with 1
            exposed_path=self._get_exposed_path(metadata),
            ingest_mode=self.ingest_mode,
            user_meta=json.dumps({"hidra": metadata}),
            substream=substream,
            callback=self._callback
        )

        if self.data_type == "metadata":
            config["local_path"] = local_path
#        elif self.data_type == "data":
#            config["data"] = data
        else:
            raise utils.NotSupported("No correct data_type was specified. "
                                     "Was setup method executed?")

        producer.send_file(**config)

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

        try:
            stream = matched["stream"]
        except KeyError:
            if self.stream:
                stream = None
            else:
                raise utils.UsageError("Missing entry for stream in matched "
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

        return stream, int(scan_id), int(file_id)

    def _callback(self, header, err):
        self.lock.acquire()
        if err is None:
            self.log.debug("Successfully sent: %s", header)
        else:
            self.log.error("Could not sent: %s, %s", header, err)
        self.lock.release()

    def stop(self):
        """ Clean up """

        for _, info in iteritems(self.stream_info):
            info["producer"].wait_requests_finished(2000)
