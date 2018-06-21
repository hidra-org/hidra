from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import os
import time
import requests
import collections
import socket

from eventdetectorbase import EventDetectorBase

__author__ = ('Manuela Kuhn <manuela.kuhn@desy.de>',
              'Jan Garrevoet <jan.garrevoet@desy.de>')


class EventDetector(EventDetectorBase):

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
        self.log.debug("Getting files from: {}".format(self.det_url))
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
        except:
            self.log.error("Error in getting file list from {0}"
                           .format(self.det_url), exc_info=True)
            # Wait till next try to prevent denial of service
            time.sleep(self.sleep_time)
            return event_message_list

        try:
            response.raise_for_status()
#            self.log.debug("response: {}".format(response.text))
            files_stored = response.json()
#            self.log.debug("files_stored: {}".format(files_stored))
        except:
            self.log.error("Getting file list...failed.", exc_info=True)
            # Wait till next try to prevent denial of service
            time.sleep(self.sleep_time)
            return event_message_list

        if (not files_stored
                or set(files_stored).issubset(self.files_downloaded)):
            # no new files received
            time.sleep(self.sleep_time)

        for f in files_stored:
            if (f.startswith(tuple(self.config["fix_subdirs"]))
                    and f not in self.files_downloaded):
                (relative_path, filename) = os.path.split(f)
                event_message = {
                    "source_path": "http://{}/data".format(self.det_ip),
                    "relative_path": relative_path,
                    "filename": filename
                }
                self.log.debug("event_message {}".format(event_message))
                event_message_list.append(event_message)
                self.files_downloaded.append(f)

        return event_message_list

    def stop(self):
        """Implementation of the abstract method stop.
        """
        pass
