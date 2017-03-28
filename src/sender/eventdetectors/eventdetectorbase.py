from __future__ import unicode_literals

import sys
import abc

# source: http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

from __init__ import BASE_PATH
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetectorBase():

    def __init__(self, config, log_queue, logger_name):
        """Initial setup

        Checks if all required parameters are set in the configuration
        """

        self.log = helpers.get_logger(logger_name, log_queue)

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

    @abc.abstractmethod
    def get_new_event(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
