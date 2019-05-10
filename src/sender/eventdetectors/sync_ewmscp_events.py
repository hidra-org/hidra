from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import collections
import copy
import json
from kafka import KafkaConsumer, TopicPartition
import multiprocessing
import os
import sys
import time
import threading

try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))
SENDER_DIR = os.path.join(BASE_DIR, "src", "sender")

if SENDER_DIR not in sys.path:
    sys.path.insert(0, SENDER_DIR)


from eventdetectorbase import EventDetectorBase
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

synced_data = []

class Synchronizing(threading.Thread):
    def __init__(self,
                 log_queue,
                 lock,
                 config):
        threading.Thread.__init__(self)

        self.log = utils.get_logger("Synchronizing", log_queue)

        self.all_data = {}
        self.lock = lock
        self.server = config["kafka_server"]
        self.topic = config["kafka_topic"]

        self.detids = config["detids"]
        self.n_detectors = config["n_detectors"]

        self.consumer = None

        self.sync_buffer = collections.deque(maxlen=config["buffer_size"])

        self.keep_running = True

        self._setup()

    def _setup(self):
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.server,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
#            auto_offset_reset='earliest',
#            enable_auto_commit=True,
        )

    def run(self):
        global synced_data

        # messages look like
        # {
        #   "finishTime": 1556031799.7914205,
        #   "inotifyTime": 1556031799.791173,
        #   "md5sum": "",
        #   "operation": "copy",
        #   "path": "/my_dir/my_subdir/file.test",
        #   "retries": 1,
        #   "size": 43008,
        #   "source": "./my_subdir/file.test"
        # }

        while self.keep_running:

            try:
                message = self.consumer.poll(1000)

                if not message:
                    continue

                topic_partition = list(message.keys())[0]

                for msg in message[topic_partition]:
                    msg_path = pathlib.Path(msg.value["path"])

                    # determine to which detector the message belongs to
                    for detid in self.detids:
                        if msg_path.match("*{}*".format(detid)):
                            found_detector = detid
                            break

                    path_tmpl = str(msg_path).replace(found_detector, "{}")

                    self.sync_buffer.append(
                        {"path_tmpl": path_tmpl, "detid": found_detector}
                    )

                    # set full?
                    # remember position in sync_buffer for easier removal later
                    file_set = tuple(
                        (i, f) for i, f  in enumerate(self.sync_buffer)
                        if f["path_tmpl"] == path_tmpl
                    )

                    if len(file_set) == self.n_detectors:
                        self.log.debug("Full image detected: %s", path_tmpl)

                        with self.lock:
                            synced_data.append(path_tmpl)

                        # remove elements backwards to keep indices correct
                        for i in sorted(file_set, reverse=True):
                            del(self.sync_buffer[i[0]])

            except KeyboardInterrupt:
                self.log.info("KeyboardInterrupt detected.")
                raise
            except Exception:
                self.log.info("Stopping thread.")
                raise

        self.log.info("Stopped while loop in synchronizing thread")

    def stop(self):
        self.keep_running = False

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config=config,
                                   log_queue=log_queue,
                                   logger_name="sync_ewnscp_events")

        # base class sets
        #   self.config_all - all configurations
        #   self.config_ed - the config of the event detector
        #   self.config - the module specific config
        #   self.ed_type -  the name of the eventdetector module
        #   self.log_queue
        #   self.log

        self.sync_thread = None
        self.lock = None
        self.monitored_dir = None

        self.required_params = {
            "eventdetector": {
                self.ed_type: [
                    "monitored_dir",
                    "buffer_size",
                    "kafka_server",
                    "kafka_topic",
                    "detids",
                    "n_detectors"
                ]
            }
        }

        self.setup(config, log_queue)

    def setup(self, config, log_queue):

        # check that the required_params are set inside of module specific
        # config
        self.check_config()

        self.lock = threading.Lock()
        self.monitored_dir = self.config["monitored_dir"]

        self.sync_thread = Synchronizing(
            log_queue=log_queue,
            lock=self.lock,
            config=self.config
        )
        self.sync_thread.start()

    def get_new_event(self):
        global synced_data

        if synced_data:
            self.log.debug("Found synced data.")

            event_message_list = []

            with self.lock:
                for i in synced_data:
                    path = pathlib.Path(i)

                    rel_path = path.parent.relative_to(self.monitored_dir)
                    event_message = {
                        "source_path": self.monitored_dir,
                        "relative_path": rel_path.as_posix(),
                        "filename": path.name
                    }

                    event_message_list.append(event_message)

                synced_data = []
        else:
            event_message_list = []

        return event_message_list

    def stop(self):
        if self.sync_thread is not None:
            self.sync_thread.stop()
            self.sync_thread = None

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
