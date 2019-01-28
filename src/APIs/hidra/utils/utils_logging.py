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
#

"""
This module provides utilities use thoughout different parts of hidra.
"""

from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import logging
import logging.handlers
import platform
import sys
import traceback

from logutils.queue import QueueListener, QueueHandler


def is_windows():
    """Determines if code is run on a windows system.

    Returns:
        True if on windows, False otherwise.
    """

    return platform.system() == "Windows"


# ------------------------------ #
#            Logging             #
# ------------------------------ #

# http://stackoverflow.com/questions/25585518/python-logging-logutils-with-queuehandler-and-queuelistener#25594270
class CustomQueueListener(QueueListener):
    """
    Overcome the limitation in the QueueListener implementation
    concerning independent setting of log levels for two handler.
    """

    # pylint: disable=invalid-name

    def __init__(self, queue, *handlers):
        """Initialize an instance with the specified queue and handlers.
        """

        super(CustomQueueListener, self).__init__(queue, *handlers)

        # Changing this to a list from tuple in the parent class
        self.handlers = list(handlers)

    def handle(self, record):
        """Override handle a record.

        This just loops through the handlers offering them the record
        to handle.

        Args:
            record: The record to handle.
        """
        record = self.prepare(record)
        for handler in self.handlers:
            # This check is not in the parent class
            if record.levelno >= handler.level:
                handler.handle(record)

    def addHandler(self, hdlr):  # noqa: N802
        """Add the specified handler to this logger.
        """
        if hdlr not in self.handlers:
            self.handlers.append(hdlr)

    def removeHandler(self, hdlr):  # noqa: N802
        """Remove the specified handler from this logger.
        """
        if hdlr in self.handlers:
            hdlr.close()
            self.handlers.remove(hdlr)


def get_stream_log_handler(loglevel="debug", datafmt=None, fmt=None):
    """Initalizes a stream handler and formats it.

    Args:
        log_level: Which log level to be used (e.g. debug).
        datafmt: The data format to be used.
        fmt: The format of the output messages.

    Returns:
        A logging StreamHandler instance with configured log level and
        output format.
    """

    loglevel = loglevel.lower()

    # check log_level
    supported_loglevel = ["debug", "info", "warning", "error", "critical"]
    if loglevel not in supported_loglevel:
        logging.error("Logging on Screen: Option %s is not supported.",
                      loglevel)
        sys.exit(1)

    # set format
    if datafmt is None:
        datefmt = "%Y-%m-%d %H:%M:%S"
    if fmt is None:
        if loglevel == "debug":
            fmt = ("[%(asctime)s] > [%(name)s] > "
                   "[%(filename)s:%(lineno)d] %(message)s")
        else:
            fmt = "[%(asctime)s] > %(message)s"

    # convert log level corresponding logging equivalent
    if loglevel == "critical":
        loglvl = logging.CRITICAL
    elif loglevel == "error":
        loglvl = logging.ERROR
    elif loglevel == "warning":
        loglvl = logging.WARNING
    elif loglevel == "info":
        loglvl = logging.INFO
    else:
        loglvl = logging.DEBUG

    formatter = logging.Formatter(datefmt=datefmt, fmt=fmt)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(loglvl)

    return handler


def get_file_log_handler(logfile,
                         logsize,
                         loglevel="debug",
                         datafmt=None,
                         fmt=None):
    """Initalizes a file handler and formats it.

    Args:
        logfile: The name of the log file.
        logsize: At which size the log file should be rotated (Linux only).
        log_level: Which log level to be used (e.g. debug).
        datafmt: The data format to be used.
        fmt: The format of the output messages.

    Returns:
        A logging FileHandler instance with configured log level and
        output format.
        Windows: there is no size limitation to the log file
        Linux: The file is rotated once it exceeds the 'logsize' defined.
               (total number of backup count is 5).
    """
    # pylint: disable=redefined-variable-type

    # set format
    if datafmt is None:
        datefmt = "%Y-%m-%d %H:%M:%S"
    if fmt is None:
        fmt = ("[%(asctime)s] "
               "[%(module)s:%(funcName)s:%(lineno)d] "
               "[%(name)s] [%(levelname)s] %(message)s")

    # convert log level corresponding logging equivalent
    if loglevel == "critical":
        loglevel = logging.CRITICAL
    elif loglevel == "error":
        loglevel = logging.ERROR
    elif loglevel == "warning":
        loglevel = logging.WARNING
    elif loglevel == "info":
        loglevel = logging.INFO
    else:
        loglevel = logging.DEBUG

    # Setup file handler to output to file
    # argument for RotatingFileHandler: filename, mode, maxBytes, backupCount)
    # 1048576 = 1MB
    if is_windows():
        handler = logging.FileHandler(logfile, 'a')
    else:
        handler = logging.handlers.RotatingFileHandler(logfile,
                                                       mode='a',
                                                       maxBytes=logsize,
                                                       backupCount=5)
    formatter = logging.Formatter(datefmt=datefmt, fmt=fmt)
    handler.setFormatter(formatter)
    handler.setLevel(loglevel)

    return handler


def get_log_handlers(logfile, logsize, verbose, onscreen_loglevel=False):
    """ Get the log Configuration for the listener

    Args:
        logfile: The name of the log file.
        logsize: At which size the log file should be rotated (Linux only).
        log_level: Which log level to be used (e.g. debug).
        datafmt: The data format to be used.
        fmt: The format of the output messages.

    Returns:
        A logging FileHandler instance with configured log level and output
        format. If onscreen_loglevel is set an additional logging StreamHandler
        instance is configured.
        The FileHandler specifics vary for different operating systems.
            Windows: There is no size limitation to the log file.
            Linux: The file is rotated once it exceeds the 'logsize' defined.
                   (total number of backup count is 5).
    """

    # Enable more detailed logging if verbose-option has been set
    if verbose:
        file_loglevel = "debug"
    else:
        file_loglevel = "info"
    file_handler = get_file_log_handler(logfile=logfile,
                                        logsize=logsize,
                                        loglevel=file_loglevel)

    # Setup stream handler to output to console
    if onscreen_loglevel:
        screen_loglevel = onscreen_loglevel.lower()  # pylint:disable=no-member

        if screen_loglevel == "debug":
            if not verbose:
                logging.error("Logging on Screen: Option DEBUG in only "
                              "active when using verbose option as well "
                              "(Fallback to INFO).")

        screen_handler = get_stream_log_handler(loglevel=screen_loglevel)
        return (file_handler, screen_handler)
    else:
        return (file_handler,)


def get_logger(logger_name, queue=False, log_level="debug"):
    """Send all logs to the main process.

    The worker configuration is done at the start of the worker process run.
    Note that on Windows you can't rely on fork semantics, so each process
    will run the logging configuration code when it starts.
    """
    # pylint: disable=redefined-variable-type

    loglevel = log_level.lower()

    if queue:
        # Create log and set handler to queue handle
        handler = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger(logger_name)
        logger.propagate = False
        logger.addHandler(handler)

        if loglevel == "debug":
            logger.setLevel(logging.DEBUG)
        elif loglevel == "info":
            logger.setLevel(logging.INFO)
        elif loglevel == "warning":
            logger.setLevel(logging.WARNING)
        elif loglevel == "error":
            logger.setLevel(logging.ERROR)
        elif loglevel == "critical":
            logger.setLevel(logging.CRITICAL)
    else:
        logger = LoggingFunction(loglevel)

    return logger


def init_logging(filename, verbose, onscreen_loglevel=False):
    """

    Args:
        filename (str): The absolute file path of the log file.
        verbose (bool):  If verbose mode should be used.
        oncreen_loglevel (bool, optional): If the log messages should be
                                           printed to screen.
    """

    # see https://docs.python.org/2/howto/logging-cookbook.html

    # more detailed logging if verbose-option has been set
    file_loglevel = logging.INFO
    if verbose:
        file_loglevel = logging.DEBUG

    # Set format
    datefmt = "%Y-%m-%d_%H:%M:%S"
#    filefmt = ("[%(asctime)s] "
#               "[%(module)s:%(funcName)s:%(lineno)d] "
#               "[%(name)s] [%(levelname)s] %(message)s")
    filefmt = ("%(asctime)s "
               "%(processName)-10s "
               "%(name)s %(levelname)-8s %(message)s")
#    filefmt = ("[%(asctime)s] [PID %(process)d] "
#               "[%(filename)s] "
#               "[%(module)s:%(funcName)s:%(lineno)d] "
#               "[%(name)s] [%(levelname)s] %(message)s")

    # log everything to file
    logging.basicConfig(level=file_loglevel,
                        format=filefmt,
                        datefmt=datefmt,
                        filename=filename,
                        filemode="a")

#        file_handler = logging.FileHandler(filename=filename,
#                                           mode="a")
#        file_handler_format = logging.Formatter(datefmt=datefmt,
#                                                fmt=filefmt)
#        file_handler.setFormatter(file_handler_format)
#        file_handler.setLevel(file_log_level)
#        logging.getLogger("").addHandler(file_andler)

    # log info to stdout, display messages with different format than the
    # file output
    if onscreen_loglevel:
        screen_loglevel = onscreen_loglevel.lower()  # pylint:disable=no-member

        if screen_loglevel == "debug" and not verbose:
            logging.error("Logging on Screen: Option DEBUG in only "
                          "active when using verbose option as well "
                          "(Fallback to INFO).")

        screen_handler = get_stream_log_handler(loglevel=screen_loglevel)
        logging.getLogger("").addHandler(screen_handler)


class LoggingFunction(object):
    """Overwrites logging with print or suppresses it.
    """

    def __init__(self, level="debug"):
        if level == "debug":
            # using output
            self.debug = self.out
            self.info = self.out
            self.warning = self.out
            self.error = self.out
            self.critical = self.out
        elif level == "info":
            # using no output
            self.debug = self.no_out
            # using output
            self.info = self.out
            self.warning = self.out
            self.error = self.out
            self.critical = self.out
        elif level == "warning":
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            # using output
            self.warning = self.out
            self.error = self.out
            self.critical = self.out
        elif level == "error":
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            self.warning = self.no_out
            # using output
            self.error = self.out
            self.critical = self.out
        elif level == "critical":
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            self.warning = self.no_out
            self.error = self.no_out
            # using output
            self.critical = self.out
        elif level is None:
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            self.warning = self.no_out
            self.error = self.no_out
            self.critical = self.no_out

    def out(self, msg, *args, **kwargs):
        """Prints to screen.

        Args:
            msg: The message to print.
            args: The arguments to fill in into msg.
            exc_info: If a traceback should be printed in addition.

        """

        # pylint: disable=no-self-use

        msg = str(msg)
        if args:
            msg = msg % args

        if "exc_info" in kwargs and kwargs["exc_info"]:
            print(msg, traceback.format_exc())
        else:
            print(msg)

    def no_out(self, msg, *args, **kwargs):
        """Print nothing.
        """
        pass
