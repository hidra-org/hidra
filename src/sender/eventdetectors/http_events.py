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
"""

# pylint: disable=broad-except

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


class EventDetector(EventDetectorBase):
    """
    Implements an event detector to get files from the Eiger detector or other
    detectors with a http interface.
    """

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config,
                                   log_queue,
                                   "http_events")

        self.config = config
        self.log.queue = log_queue

        self.session = None
        self.det_ip = None
        self.det_api_version = None
        self.det_url = None

        # time to sleep after detector returned emtpy file list
        self.sleep_time = 0.5
        self.files_downloaded = None

        self.required_params = ["det_ip",
                                "det_api_version",
                                "history_size",
                                "fix_subdirs"]

        self.check_config()
        self.setup()

    def setup(self):
        """
        Sets static configuration parameters and sets up ring buffer.
        """

        self.session = requests.session()

        # Enable specification via IP and DNS name
        self.det_ip = socket.gethostbyaddr(self.config["det_ip"])[2][0]
        self.det_api_version = self.config["det_api_version"]
        self.det_url = ("http://{}/filewriter/api/{}/files"
                        .format(self.det_ip, self.det_api_version))
        self.log.debug("Getting files from: %s", self.det_url)
#            http://192.168.138.37/filewriter/api/1.6.0/files

        # history to prevend double events
        self.files_downloaded = collections.deque(
            maxlen=self.config["history_size"]
        )

    def get_new_event(self):
        """Implementation of the abstract method get_new_event.
        """

        event_message_list = []

        files_stored = []

#        try:
#            # returns a tuble of the form:
#            # ('testp06/37_data_000001.h5', 'testp06/37_master.h5',
#            #  'testp06/36_data_000003.h5', 'testp06/36_data_000002.h5',
#            #  'testp06/36_data_000001.h5', 'testp06/36_master.h5')
#            files_stored = self.detdevice.read_attribute(
#                "FilesInBuffer", timeout=3
#            ).value
#        except Exception as e:
#            self.log.error("Getting 'FilesInBuffer'...failed. {}".format(e))
#            time.sleep(0.2)
#            return event_message_list

        try:
            response = self.session.get(self.det_url)
        except KeyboardInterrupt:
            return event_message_list
        except Exception:
            self.log.error("Error in getting file list from %s", self.det_url,
                           exc_info=True)
            # Wait till next try to prevent denial of service
            time.sleep(self.sleep_time)
            return event_message_list

        try:
            response.raise_for_status()
#            self.log.debug("response: {}".format(response.text))
            files_stored = response.json()
#            self.log.debug("files_stored: {}".format(files_stored))
        except Exception:
            self.log.error("Getting file list...failed.", exc_info=True)
            # Wait till next try to prevent denial of service
            time.sleep(self.sleep_time)
            return event_message_list

        if (not files_stored
                or set(files_stored).issubset(self.files_downloaded)):
            # no new files received
            time.sleep(self.sleep_time)

        fix_subdirs_tuple = tuple(self.config["fix_subdirs"])
        for file_obj in files_stored:
            if (file_obj.startswith(fix_subdirs_tuple)
                    and file_obj not in self.files_downloaded):
                (relative_path, filename) = os.path.split(file_obj)
                event_message = {
                    "source_path": "http://{}/data".format(self.det_ip),
                    "relative_path": relative_path,
                    "filename": filename
                }
                self.log.debug("event_message %s", event_message)
                event_message_list.append(event_message)
                self.files_downloaded.append(file_obj)

        return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """
        pass
