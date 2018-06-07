from __future__ import print_function
from __future__ import unicode_literals

from eventdetectorbase import EventDetectorBase
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config,
                                   log_queue,
                                   "eventdetector_template")

        required_params = []

        # Check format of config
        check_passed, config_reduced = utils.check_config(required_params,
                                                          config,
                                                          self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {}"
                          .format(config_reduced))
        else:
            # self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    def get_new_event(self):

        event_message_list = [{
            "source_path": "/source",
            "relative_path": "relpath/to/dir",
            "filename": "my_file.cbf"
        }]

        self.log.debug("event_message: {}".format(event_message_list))

        return event_message_list

    def stop(self):
        pass


if __name__ == '__main__':
    from multiprocessing import Queue
    import os
    import logging
    import tempfile
    from logutils.queue import QueueHandler
    from __init__ import BASE_PATH

    logfile = os.path.join(BASE_PATH, "logs", "eventdetector_template.log")
    logsize = 10485760

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = utils.get_log_handlers(logfile,
                                    logsize,
                                    verbose=True,
                                    onscreen_loglevel="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = utils.CustomQueueListener(log_queue, h1, h2)
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
                logging.debug("event_list: {}".format(event_list))
        except KeyboardInterrupt:
            break

    log_queue.put_nowait(None)
    log_queue_listener.stop()
