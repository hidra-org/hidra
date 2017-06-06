from __future__ import print_function
from __future__ import unicode_literals

import os
import time
import requests
import collections
import socket

from eventdetectorbase import EventDetectorBase
import helpers

__author__ = ('Manuela Kuhn <manuela.kuhn@desy.de>',
              'Jan Garrevoet <jan,garrevoet@desy.de>')


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self, config, log_queue,
                                   "http_events")

        required_params = ["det_ip",
                           "det_api_version",
                           "history_size"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            config,
                                                            self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {0}"
                          .format(config_reduced))

            self.session = requests.session()

            # Enable specification via IP and DNS name
            self.det_ip = socket.gethostbyaddr(config["det_ip"])[2][0]
            self.det_api_version = config["det_api_version"]
            self.det_url = ("http://{0}/filewriter/api/{1}/files"
                            .format(self.det_ip,
                                    self.det_api_version))
            self.log.debug("Getting files from: {0}".format(self.det_url))
#            http://192.168.138.37/filewriter/api/1.6.0/files

            # time to sleep after detector returned emtpy file list
            self.sleep_time = 0.5

            # history to prevend double events
            self.files_downloaded = collections.deque(
                maxlen=config["history_size"])

        else:
            # self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    def get_new_event(self):

        event_message_list = []

        files_stored = []

#        try:
#            # returns a tuble of the form:
#            # ('testp06/37_data_000001.h5', 'testp06/37_master.h5',
#            #  'testp06/36_data_000003.h5', 'testp06/36_data_000002.h5',
#            #  'testp06/36_data_000001.h5', 'testp06/36_master.h5')
#            files_stored = self.detdevice.read_attribute(
#                "FilesInBuffer", timeout=3).value
#        except Exception as e:
#            self.log.error("Getting 'FilesInBuffer'...failed. {0}".format(e))
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
#            self.log.debug("response: {0}".format(response.text))
            files_stored = response.json()
#            self.log.debug("files_stored: {0}".format(files_stored))
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
            if f not in self.files_downloaded:
                (relative_path, filename) = os.path.split(f)
                event_message = {
                    "source_path": "http://{0}/data".format(self.det_ip),
                    "relative_path": relative_path,
                    "filename": filename
                }
                self.log.debug("event_message {0}".format(event_message))
                event_message_list.append(event_message)
                self.files_downloaded.append(f)

        return event_message_list

    def stop(self):
        pass


if __name__ == '__main__':
    from multiprocessing import Queue
    from __init__ import BASE_PATH
    from logutils.queue import QueueHandler
    import logging

    logfile = os.path.join(BASE_PATH, "logs", "http_events.log")
    logsize = 10485760

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True,
                                      onscreen_log_level="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = helpers.CustomQueueListener(log_queue, h1, h2)
    log_queue_listener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

#    detectorDevice   = "haspp10lab:10000/p10/eigerdectris/lab.01"
#    detectorDevice   = "haspp06:10000/p06/eigerdectris/exp.01"
#    filewriterDevice = "haspp10lab:10000/p10/eigerfilewriter/lab.01"
#    filewriterDevice = "haspp06:10000/p06/eigerfilewriter/exp.01"
#    det_ip          = "192.168.138.52"  # haspp11e1m
    det_ip = "asap3-mon"
    det_api_version = "1.5.0"
    config = {
        "det_ip": det_ip,
        "det_api_version": det_api_version,
        "history_size": 1000
    }

    eventdetector = EventDetector(config, log_queue)

    for i in range(3):
        try:
            event_list = eventdetector.get_new_event()
            if event_list:
                print("event_list:", event_list)

            time.sleep(1)
        except KeyboardInterrupt:
            break

    log_queue.put_nowait(None)
    log_queue_listener.stop()
