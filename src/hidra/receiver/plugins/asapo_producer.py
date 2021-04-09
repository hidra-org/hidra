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
    data_source: string  # optional
    token: string
    n_threads: int
    file_regex: regex string
    user_config_path: : string # path to user config file

Example config:
    asapo_producer:
        endpoint: "asapo-services:8400"
        beamtime: "asapo_test"
        data_source: "hidra_test"
        token: "KmUDdacgBzaOD3NIJvN1NmKGqWKtx0DK-NyPjdpeWkc="
        n_threads: 1
        user_config_path: 'path/to/conf'
        file_regex: '.*/(?P<data_source>.*)/scan_(?P<scan_id>.*)/'
                    '(?P<file_idx_in_scan>.*).tif'
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
from time import time, sleep

from future.utils import iteritems

import asapo_producer
import hidra.utils as utils

try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path

logger = logging.getLogger(__name__)


def get_exposed_path(metadata):
    exposed_path = Path(metadata["relative_path"],
                        metadata["filename"]).parts
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


def get_entry(dict_obj, name, default=None):
    try:
        return dict_obj[name]
    except KeyError:
        if default is not None:
            return default
        else:
            raise utils.UsageError(f"Missing entry for {name} in matched result")


def get_ingest_mode(mode):
    if mode == "INGEST_MODE_TRANSFER_METADATA_ONLY":
        return asapo_producer.INGEST_MODE_TRANSFER_METADATA_ONLY
    else:
        raise utils.NotSupported("Ingest mode '{}' is not supported"
                                 .format(mode))


def check_config(config, required_parameter):
    failed = False
    for i in required_parameter:
        if i not in config:
            logger.error("Wrong configuration. Missing parameter: '%s'", i)
            failed = True

    if failed:
        raise utils.WrongConfiguration("The configuration has missing or wrong parameters.")


def parse_file_path(file_regex, file_path):
    # parse file name
    search = re.search(file_regex, file_path)
    if search:
        return search.groupdict()
    else:
        logger.debug("file name: %s", file_path)
        logger.debug("file_regex: %s", file_regex)
        raise Ignored("Does not match file pattern. Ignoring file {}".format(file_path))


class Ignored(Exception):
    """Raised when an event is processed that should be ignored."""
    pass


class Plugin(object):
    """Implements an ASAP::O producer plugin
    """

    def __init__(self, plugin_config):
        super().__init__()

        self.config = plugin_config

        self.timeout = 1
        self.config_timeout = 10
        self.config_time = 0
        self.check_time = 0

        self.asapo_worker = None

    def setup(self):
        """Sets the configuration and starts the producer
        """
        required_parameter = [
            "endpoint",
            "n_threads",
            "token",
            "user_config_path",
            "file_regex",
            "beamtime"
        ]
        check_config(self.config, required_parameter)

        if "data_source" in self.config:
            logger.debug("Static data_source configured. Using: %s", self.config["data_source"])

        if "token" in self.config:
            logger.debug("Static token configured.")

    def get_data_type(self):
        return "metadata"

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
            config = self._create_asapo_config()
            self.asapo_worker = AsapoWorker(**config)

        self.asapo_worker.send_message(local_path, metadata)

    def _create_asapo_config(self):
        config = {k: v for k, v in self.config.items() if k != "user_config_path"}
        try:
            user_config = utils.load_config(self.config["user_config_path"])
            config.update(user_config)
        except OSError as err:
            logger.warning("Could not get user config: {}".format(err))
            logger.warning("Default config is used")
        return config

    def _get_config_time(self, file_path):
        start = time()
        while True:
            try:
                return path.getmtime(file_path)
            except OSError as err:
                if time() - start > self.config_timeout:
                    logger.warning("Could not get creation time of user config: {}".format(err))
                    return 0
                sleep(1)

    def _config_is_modified(self):
        ts = time()
        if (ts - self.check_time) > self.timeout:
            file_path = self.config["user_config_path"]
            config_time = self._get_config_time(file_path)
            if self.config_time != config_time:
                self.config_time = config_time
                return True
        self.check_time = ts
        return False

    def stop(self):
        """ Clean up """
        self.asapo_worker.stop()


class AsapoWorker:
    def __init__(self, endpoint, beamtime, token, n_threads, file_regex,
                 data_source=None, timeout=5):
        self.endpoint = endpoint
        self.beamtime = beamtime
        self.token = token
        self.n_threads = n_threads
        self.timeout = timeout
        self.data_source = data_source
        self.file_regex = file_regex

        # Other ingest modes are not yet implemented
        self.ingest_mode = get_ingest_mode("INGEST_MODE_TRANSFER_METADATA_ONLY")
        self.lock = threading.Lock()
        self.data_source_info = {}

    def _create_producer(self, data_source):
        logger.info("Create producer with data_source=%s", data_source)
        self.data_source_info[data_source] = {
            "producer": asapo_producer.create_producer(self.endpoint, "raw", self.beamtime, "auto",
                                                       data_source, self.token, self.n_threads,
                                                       self.timeout * 1000),
        }

    def _get_producer(self, data_source):
        if data_source not in self.data_source_info:
            self._create_producer(data_source=data_source)
        return self.data_source_info[data_source]["producer"]

    def send_message(self, local_path, metadata):

        try:
            data_source, stream, file_idx = self._parse_file_name(local_path)
            logger.debug("using stream %s", stream)
        except Ignored:
            logger.debug("Ignoring file %s", local_path)
            return

        producer = self._get_producer(data_source)
        producer.send(id=file_idx + 1,  # files start with index 0 and asapo with 1
                      exposed_path=get_exposed_path(metadata),
                      data=None,
                      user_meta=json.dumps({"hidra": metadata}),
                      ingest_mode=self.ingest_mode,
                      stream=stream,
                      callback=self._callback)

    def _callback(self, header, err):
        self.lock.acquire()
        header = {key: val for key, val in header.items() if key != 'data'}
        if err is None:
            logger.debug("Successfully sent: %s", header)
        else:
            logger.error("Could not sent: %s, %s", header, err)
        self.lock.release()

    def _parse_file_name(self, path):
        matched = parse_file_path(self.file_regex, path)
        data_source = get_entry(matched, "data_source", self.data_source)
        stream = get_entry(matched, "scan_id")
        file_idx = int(get_entry(matched, "file_idx_in_scan"))
        return data_source, stream, file_idx

    def stop(self):
        for _, info in iteritems(self.data_source_info):
            info["producer"].wait_requests_finished(2000)
