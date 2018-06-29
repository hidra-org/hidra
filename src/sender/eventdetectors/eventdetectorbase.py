from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import sys
import abc
import __init__  # noqa F401
import utils
from utils import WrongConfiguration
from base_class import Base

# source:
# http://stackoverflow.com/questions/35673474/using-abc-abcmeta-in-a-way-it-is-compatible-both-with-python-2-7-and-python-3-5  # noqa E501
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta(str("ABC"), (), {})

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class EventDetectorBase(Base):

    def __init__(self, config, log_queue, logger_name):  # noqa F811
        """Initial setup

        Args:
            config (dict): A dictionary containing the configuration
                           parameters.
            log_queue: The multiprocessing queue which is used for logging.
            logger_name (str): The name to be used for the logger.
        """

        self.config = config
        self.log_queue = log_queue

        self.log = utils.get_logger(logger_name, log_queue)

        self.required_params = []

    def check_config(self):
        """Check that the configuration containes the nessessary parameters.

        Raises:
            WrongConfiguration: The configuration has missing or
                                wrong parameteres.
        """

        # Check format of config
        check_passed, config_reduced = utils.check_config(self.required_params,
                                                          self.config,
                                                          self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {}"
                          .format(config_reduced))
        else:
            # self.log.debug("config={}".format(self.config))
            msg = "The configuration has missing or wrong parameteres."
            raise WrongConfiguration(msg)

    @abc.abstractmethod
    def get_new_event(self):
        """Get the events that happened since the last request.

        Returns:
            A list of events. Each event is a dictionary of the from:
            {
                "source_path": ...
                "relative_path": ...
                "filename": ...
            }
        """
        pass

    @abc.abstractmethod
    def stop(self):
        """Stop and clean up.
        """
        pass

    def __exit__(self, type, value, traceback):
        self.stop()

    def __del__(self):
        self.stop()
