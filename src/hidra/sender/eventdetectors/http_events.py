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
#     Jan Garrevoet <jan.garrevoet@desy.de>
#

"""
This module implements the event detector used for the Eiger detector and
other detectors with a http interface

Needed configuration in config file:
eventdetector:
    type: http_events
    http_events:
        fix_subdirs: list of strings
        history_size: int
        det_ip: string
        det_api_version: string

Example config:
    http_events:
        fix_subdirs:
            - "commissioning/raw"
            - "commissioning/scratch_bl"
            - "current/raw"
            - "current/scratch_bl"
            - "local"
        history_size: 2000
        det_ip: asap3-mon
        det_api_version: 1.6.0
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import collections
import os
import socket
import time
import requests

from eventdetectorbase import EventDetectorBase

__author__ = ('Manuela Kuhn <manuela.kuhn@desy.de>',
              'Jan Garrevoet <jan.garrevoet@desy.de>')


class HTTPConnection:
    def __init__(self, session):
        self.session = session

    def get_file_list(self, url):
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()


class FileFilterDeque:
    def __init__(self, fix_subdirs, history_size):
        self.fix_subdirs = tuple(fix_subdirs)
        # history to prevent double events
        self.files_seen = collections.deque(maxlen=history_size)

    def get_new_files(self, files_stored):
        new_files = []
        if (not files_stored
                or set(files_stored).issubset(self.files_seen)):
            return []

        for file_obj in files_stored:
            if (file_obj.startswith(self.fix_subdirs)
                    and file_obj not in self.files_seen):
                self.files_seen.append(file_obj)
                new_files.append(file_obj)

        return new_files


class FileFilterSet:
    def __init__(self, fix_subdirs):
        self.fix_subdirs = tuple(fix_subdirs)
        # history to prevent double events
        self.files_seen = set()

    def get_new_files(self, files_stored):
        filtered_files = []
        for file_obj in files_stored:
            if (file_obj.startswith(self.fix_subdirs)
                    and file_obj not in self.files_seen):
                filtered_files.append(file_obj)

        self.files_seen = set(files_stored)

        return filtered_files


class EventDetector(EventDetectorBase):
    """
    Implements an event detector to get files from the Eiger detector or other
    detectors with a http interface.
    """

    def __init__(self, eventdetector_base_config):

        EventDetectorBase.__init__(self, eventdetector_base_config,
                                   name=__name__)

        # base class sets
        #   self.config_all - all configurations
        #   self.config_ed - the config of the event detector
        #   self.config - the module specific config
        #   self.ed_type -  the name of the eventdetector module
        #   self.log_queue
        #   self.log

        self.required_params = ["det_ip",
                                "det_api_version",
                                "history_size",
                                "fix_subdirs"]

        # check that the required_params are set inside of module specific
        # config
        self.check_config()

        self.event_detector_impl = create_eventdetector_impl(
            log=self.log, **self.config)

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """
        return self.event_detector_impl.get_new_event()

    def stop(self):
        """Implementation of the abstract method stop.
        """
        pass


def resolve_ip(ip):
    return socket.gethostbyaddr(ip)[2][0]


def create_eventdetector_impl(
        det_ip, det_api_version, history_size, fix_subdirs, log,
        file_writer_url=(
            "http://{det_ip}/filewriter/api/{det_api_version}/files/"),
        data_url="http://{det_ip}/data"):

    det_ip = resolve_ip(det_ip)

    file_writer_url = file_writer_url.format(
        det_ip=det_ip, det_api_version=det_api_version)
    data_url = data_url.format(det_ip=det_ip)

    log.debug("Getting files from: %s", file_writer_url)

    session = requests.session()
    connection = HTTPConnection(session)

    if history_size >= 0:
        file_filter = FileFilterDeque(fix_subdirs, history_size)
    else:
        file_filter = FileFilterSet(fix_subdirs)

    return EventDetectorImpl(
        file_writer_url=file_writer_url,
        data_url=data_url,
        connection=connection,
        file_filter=file_filter,
        log=log)


class EventDetectorImpl:
    def __init__(
            self, file_writer_url, data_url, connection, file_filter, log):
        self.log = log

        # time to sleep after detector returned emtpy file list
        self.sleep_time = 0.5
        # Enable specification via IP and DNS name

        self.file_writer_url = file_writer_url
        self.data_url = data_url

        self.connection = connection

        self.file_filter = file_filter

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """

        files_stored = self._get_files_stored()
        if not files_stored:
            # Wait till next try to prevent denial of service
            time.sleep(self.sleep_time)
            return []

        new_files = self.file_filter.get_new_files(files_stored)

        if not new_files:
            # no new files received
            time.sleep(self.sleep_time)
            return []

        event_message_list = self._build_event_messages(new_files)

        return event_message_list

    def _get_files_stored(self):
        try:
            files_stored = self.connection.get_file_list(self.file_writer_url)
        except Exception:
            self.log.error("Error in getting file list from %s",
                           self.file_writer_url, exc_info=True)
            return []

        # api version 1.8.0 and newer return a dictionary instead of a list
        if isinstance(files_stored, dict):
            files_stored = files_stored["value"]

        # api version 1.8.0 and newer can return None instead of an empty list
        if files_stored is None:
            files_stored = []

        return files_stored

    def _build_event_messages(self, new_files):
        event_message_list = []
        for file_obj in new_files:
            (relative_path, filename) = os.path.split(file_obj)
            event_message = {
                "source_path": self.data_url,
                "relative_path": relative_path,
                "filename": filename
            }
            self.log.debug("event_message %s", event_message)
            event_message_list.append(event_message)
        return event_message_list
