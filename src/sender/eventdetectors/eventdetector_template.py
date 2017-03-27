from __future__ import print_function
from __future__ import unicode_literals

import os
import logging
import tempfile

from logutils.queue import QueueHandler

from __init__ import BASE_PATH
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetector():

    def __init__(self, config, log_queue):

        self.log = helpers.get_logger("eventdetector_template", log_queue)

        required_params = []

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            config,
                                                            self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {0}"
                          .format(config_reduced))
        else:
            self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    def get_new_event(self):

        event_message_list = [{
            "source_path": "/source",
            "relative_path": "relpath/to/dir",
            "filename": "my_file.cbf"
        }]

        self.log.debug("event_message: {0}".format(event_message_list))

        return event_message_list

    def stop(self):
        pass

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    from multiprocessing import Queue

    logfile = os.path.join(BASE_PATH, "logs", "eventdetector_template.log")
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

    ipc_path = os.path.join(tempfile.gettempdir(), "hidra")

    config = dict()

    eventdetector = EventDetector(config, log_queue)

    i = 100
    while i <= 101:
        try:
            i += 1
            event_list = eventdetector.get_new_event()
            if event_list:
                logging.debug("event_list: {0}".format(event_list))
        except KeyboardInterrupt:
            break

    log_queue.put_nowait(None)
    log_queue_listener.stop()
