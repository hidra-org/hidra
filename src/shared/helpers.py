from __future__ import unicode_literals
from __future__ import print_function

import os
import sys
import platform
import logging
import logging.handlers
import shutil
import subprocess
import socket
import re
from _version import __version__
from cfel_optarg import parse_parameters

try:
    import ConfigParser
except ImportError:
    # The ConfigParser module has been renamed to configparser in Python 3
    import configparser as ConfigParser

try:
    # try to use the system module
    from logutils.queue import QueueListener
    from logutils.queue import QueueHandler
except:
    # there is no module logutils installed, fallback on the one in shared
    from shared import SHARED_PATH
    if SHARED_PATH not in sys.path:
        sys.path.append(SHARED_PATH)

    from logutils.queue import QueueListener
    from logutils.queue import QueueHandler

DOMAIN = ".desy.de"
LDAPURI = "it-ldap-slave.desy.de:1389"


def is_windows():
    if platform.system() == "Windows":
        return True
    # osRelease = platform.release()
    # supportedWindowsReleases = ["7"]
    # if osRelease in supportedWindowsReleases:
    #     return True
    else:
        return False


def is_linux():
    if platform.system() == "Linux":
        return True
    else:
        return False


# This function is needed because configParser always needs a section name
# the used config file consists of key-value pairs only
# source: http://stackoverflow.com/questions/2819696/
#                parsing-properties-file-in-python/2819788#2819788
class FakeSecHead (object):
    def __init__(self, fp):
        self.fp = fp
        self.sechead = '[asection]\n'

    def readline(self):
        if self.sechead:
            try:
                return self.sechead
            finally:
                self.sechead = None
        else:
            return self.fp.readline()


def str2bool(v):
    return v.lower() == "true"


def read_config(config_file):

    config = ConfigParser.RawConfigParser()
    try:
        config.readfp(FakeSecHead(open(config_file)))
    except:
        with open(config_file, 'r') as f:
            config_string = '[asection]\n' + f.read()
        config.read_string(config_string)

    return config


def set_parameters(config_file, arguments):

    params = parse_parameters(read_config(config_file))["asection"]

    # arguments set when the program is called have a higher priority than
    # the ones in the config file
    for arg in vars(arguments):
        arg_value = getattr(arguments, arg)
        if arg_value is not None:
            if type(arg_value) is str:
                if arg_value.lower() == "none":
                    params[arg] = None
                elif arg_value.lower() == "false":
                    params[arg] = False
                elif arg_value.lower() == "true":
                    params[arg] = True
                else:
                    params[arg] = arg_value
            else:
                params[arg] = arg_value

    return params


def excecute_ldapsearch(ldap_cn):
    global LDAPURI

    p = subprocess.Popen(
        ["ldapsearch",
         "-x",
         "-H ldap://" + LDAPURI,
         "cn=" + ldap_cn, "-LLL"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    lines = p.stdout.readlines()

    match_host = re.compile(r'nisNetgroupTriple: [(]([\w|\S|.]+),.*,[)]',
                            re.M | re.I)
    netgroup = []

    for line in lines:
        if match_host.match(line):
            if match_host.match(line).group(1) not in netgroup:
                netgroup.append(match_host.match(line).group(1))

    return netgroup


# http://code.activestate.com/recipes/541096-prompt-the-user-for-confirmation/
def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.

    'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.

    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n:
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y:
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True

    """

    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        try:
            ans = raw_input(prompt)
        except KeyboardInterrupt:
            logging.error("Keyboard Interruption detected.")
        except Exception as e:
            logging.error("Something went wrong with the confirmation.")
            logging.debug("Error was: {0}".format(e))
            break

        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            logging.error("please enter y or n.")
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False


def check_type(specified_type, supported_types, log_string):

    specified_type = specified_type.lower()

    if specified_type in supported_types:
        logging.debug("{0} '{1}' is ok.".format(log_string, specified_type))
    else:
        logging.error("{0} '{1}' is not supported."
                      .format(log_string, specified_type))
        sys.exit(1)


def check_dir_empty(dir_path):

    # check if directory is empty
    if os.listdir(dir_path):
        logging.debug("Directory '{0}' is not empty.".format(dir_path))
        if confirm(prompt="Directory {0} is not empty.\n"
                          "Should its content be removed?".format(dir_path),
                   resp=True):
            for element in os.listdir(dir_path):
                path = os.path.join(dir_path, element)
                if os.path.isdir(path):
                    try:
                        os.rmdir(path)
                    except OSError:
                        shutil.rmtree(path)
                else:
                    os.remove(path)
            logging.info("All elements of directory {0} were removed."
                         .format(dir_path))


def check_any_sub_dir_exists(dir_path, subdirs):

    dir_path = os.path.normpath(dir_path)
    dirs_to_check = [os.path.join(dir_path, directory)
                     for directory in subdirs]
    no_subdir = True

    for d in dirs_to_check:
        # check directory path for existance. exits if it does not exist
        if os.path.exists(d):
            no_subdir = False

    if no_subdir:
        logging.error("There are none of the specified subdirectories inside "
                      "'{0}'. Abort.".format(dir_path))
        logging.error("Checked paths: {0}".format(dirs_to_check))
        sys.exit(1)


def check_sub_dir_contained(dir_path, subdirs):
    subdir_contained = False
    for subdir in subdirs:
        if dir_path[-len(subdir):] == subdir:
            subdir_contained = True

    return subdir_contained


def check_all_sub_dir_exist(dir_path, subdirs):

    dir_path = os.path.normpath(dir_path)
    dirs_to_check = [os.path.join(dir_path, directory)
                     for directory in subdirs]

    for d in dirs_to_check:
        if not os.path.exists(d):
            logging.error("Dir '{0}' does not exist. Abort.".format(d))
            sys.exit(1)


def check_existance(path):
    # Check path for existance.
    # Exits if it does not exist
    if os.path.isdir(path):
        obj_type = "Dir"
    else:
        obj_type = "File"

    if not os.path.exists(path):
        logging.error("{0} '{1}' does not exist. Abort."
                      .format(obj_type, path))
        sys.exit(1)


def check_writable(file_to_check):
    # Exits if file can be written
    try:
        file_descriptor = open(file_to_check, "a")
        file_descriptor.close()
    except:
        logging.error("Unable to create the file {0}".format(file_to_check))
        sys.exit(1)


def check_version(version, log):
    log.debug("remote version: {0}, local version: {1}"
              .format(version, __version__))

    if version.rsplit(".", 1)[0] < __version__.rsplit(".", 1)[0]:
        log.info("Version of receiver is lower. Please update receiver.")
        return False
    elif version.rsplit(".", 1)[0] > __version__.rsplit(".", 1)[0]:
        log.info("Version of receiver is higher. Please update sender.")
        return False
    else:
        return True


def check_host(host, whitelist, log):
    global DOMAIN

    if whitelist is None:
        return True

    if host and whitelist:

        if type(host) == list:
            return_val = True
            for hostname in host:
                host_modified = hostname.replace(DOMAIN, "")

                if (hostname not in whitelist
                        and host_modified not in whitelist):
                    log.info("Host {0} is not allowed to connect"
                             .format(hostname))
                    return_val = False

            return return_val

        else:
            host_modified = host.replace(DOMAIN, "")

            if host in whitelist or host_modified in whitelist:
                return True
            else:
                log.info("Host {0} is not allowed to connect".format(host))

    return False


def check_ping(host, log=logging):
    if is_windows():
        response = os.system("ping -n 1 -w 2 {0}".format(host))
    else:
        response = os.system("ping -c 1 -w 2 {0} > /dev/null 2>&1"
                             .format(host))

    if response != 0:
        log.error("{0} is not pingable.".format(host))
        sys.exit(1)


# IP and DNS name should be both in the whitelist
def extend_whitelist(whitelist, log):
    global DOMAIN

    log.info("Configured whitelist: {0}".format(whitelist))
    extended_whitelist = []

    if whitelist is not None:
        for host in whitelist:

            if host == "localhost":
                extended_whitelist.append(socket.gethostbyname(host))
            else:
                try:
                    hostname, tmp, ip = socket.gethostbyaddr(host)

                    host_modified = hostname.replace(DOMAIN, "")

                    if host_modified not in whitelist:
                        extended_whitelist.append(host_modified)

                    if ip[0] not in whitelist:
                        extended_whitelist.append(ip[0])
                except:
                    pass

        for host in extended_whitelist:
            whitelist.append(host)

        log.debug("Extended whitelist: {0}".format(whitelist))


def check_config(required_params, config, log):
    """
    Check the configuration

    Args:

        required_params (list): list which can contain multiple formats
            - string: check if the parameter is contained
            - list of the format [<name>, <format>]: checks if the parameter
                is contained and has the right format
            - list of the format [<name>, <list of options>]: checks if the
                parameter is contained and set to supported values
        config (dict): dictionary where the configuration is stored
        log (class Logger): Logger instance of the module logging

    Returns:

        check_passed: if all checks were successfull
        config_reduced (string): string to print all required parameters with
            their values
    """

    check_passed = True
    config_reduced = "{"

    for param in required_params:
        # multiple checks have to be done
        if type(param) == list:
            # checks if the parameter is contained in the config dict
            if param[0] not in config:
                log.error("Configuration of wrong format. "
                          "Missing parameter '{0}'".format(param[0]))
                check_passed = False
            # check if the parameter is one of the supported values
            elif type(param[1]) == list:
                if config[param[0]] not in param[1]:
                    log.error("Configuration of wrong format. Options for "
                              "parameter '{0}' are {1}"
                              .format(param[0], param[1]))
                    log.debug("parameter '{0}' = {1}"
                              .format(param[0], config[param[0]]))
                    check_passed = False
            # check if the parameter has the supported type
            elif type(config[param[0]]) != param[1]:
                log.error("Configuration of wrong format. Parameter '{0}' is "
                          "of format '{1}' but should be of format '{2}'"
                          .format(param[0], type(config[param[0]]), param[1]))
                check_passed = False
        # checks if the parameter is contained in the config dict
        elif param not in config:
            log.error("Configuration of wrong format. Missing parameter: '{0}'"
                      .format(param))
            check_passed = False
        else:
            config_reduced += "{0}: {1}, ".format(param, config[param])

    # Remove redundant divider
    config_reduced = config_reduced[:-2] + "}"

    return check_passed, config_reduced


# http://stackoverflow.com/questions/25585518/
#        python-logging-logutils-with-queuehandler-and-queuelistener#25594270
class CustomQueueListener (QueueListener):
    def __init__(self, queue, *handlers):
        super(CustomQueueListener, self).__init__(queue, *handlers)
        """
        Initialise an instance with the specified queue and
        handlers.
        """
        # Changing this to a list from tuple in the parent class
        self.handlers = list(handlers)

    def handle(self, record):
        """
        Override handle a record.

        This just loops through the handlers offering them the record
        to handle.

        :param record: The record to handle.
        """
        record = self.prepare(record)
        for handler in self.handlers:
            # This check is not in the parent class
            if record.levelno >= handler.level:
                handler.handle(record)

    def addHandler(self, hdlr):  # noqa: N802
        """
        Add the specified handler to this logger.
        """
        if not (hdlr in self.handlers):
            self.handlers.append(hdlr)

    def removeHandler(self, hdlr):  # noqa: N802
        """
        Remove the specified handler from this logger.
        """
        if hdlr in self.handlers:
            hdlr.close()
            self.handlers.remove(hdlr)


# Get the log Configuration for the lisener
def get_log_handlers(logfile, logsize, verbose, onscreen_log_level=False):
    # Enable more detailed logging if verbose-option has been set
    loglevel = logging.INFO
    if verbose:
        loglevel = logging.DEBUG

    # Set format
    datef = "%Y-%m-%d %H:%M:%S"
    f = ("[%(asctime)s] [%(module)s:%(funcName)s:%(lineno)d] "
         "[%(name)s] [%(levelname)s] %(message)s")

    # Setup file handler to output to file
    # argument for RotatingFileHandler: filename, mode, maxBytes, backupCount)
    # 1048576 = 1MB
    if is_windows():
        h1 = logging.FileHandler(logfile, 'a')
    else:
        h1 = logging.handlers.RotatingFileHandler(logfile, 'a', logsize, 5)
    f1 = logging.Formatter(datefmt=datef, fmt=f)
    h1.setFormatter(f1)
    h1.setLevel(loglevel)

    # Setup stream handler to output to console
    if onscreen_log_level:
        onscreen_log_level_lower = onscreen_log_level.lower()
        if (onscreen_log_level_lower in ["debug", "info", "warning",
                                         "error", "critical"]):

            f = "[%(asctime)s] > %(message)s"

            if onscreen_log_level_lower == "debug":
                screen_log_level = logging.DEBUG
                f = "[%(asctime)s] > [%(filename)s:%(lineno)d] %(message)s"

                if not verbose:
                    logging.error("Logging on Screen: Option DEBUG in only "
                                  "active when using verbose option as well "
                                  "(Fallback to INFO).")
            elif onscreen_log_level_lower == "info":
                screen_log_level = logging.INFO
            elif onscreen_log_level_lower == "warning":
                screen_log_level = logging.WARNING
            elif onscreen_log_level_lower == "error":
                screen_log_level = logging.ERROR
            elif onscreen_log_level_lower == "critical":
                screen_log_level = logging.CRITICAL

            h2 = logging.StreamHandler()
            f2 = logging.Formatter(datefmt=datef, fmt=f)
            h2.setFormatter(f2)
            h2.setLevel(screen_log_level)

            return h1, h2
        else:
            logging.error("Logging on Screen: Option {0} is not supported."
                          .format(onscreen_log_level))
            exit(1)

    else:
        return h1


# Send all logs to the main process
# The worker configuration is done at the start of the worker process run.
# Note that on Windows you can't rely on fork semantics, so each process
# will run the logging configuration code when it starts.
def get_logger(logger_name, queue, log_level=logging.DEBUG):
    # Create log and set handler to queue handle
    h = QueueHandler(queue)  # Just the one handler needed
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    logger.addHandler(h)
    logger.setLevel(log_level)

    return logger


def init_logging(filename_full_path, verbose, onscreen_log_level=False):
    # see https://docs.python.org/2/howto/logging-cookbook.html

    # more detailed logging if verbose-option has been set
    logging_level = logging.INFO
    if verbose:
        logging_level = logging.DEBUG

    # log everything to file
#                        format=("[%(asctime)s] [PID %(process)d] "
#                                "[%(filename)s] "
#                                "[%(module)s:%(funcName)s:%(lineno)d] "
#                                "[%(name)s] [%(levelname)s] %(message)s"),
    logging.basicConfig(level=logging_level,
                        format=("%(asctime)s %(processName)-10s %(name)s "
                                "%(levelname)-8s %(message)s"),
                        datefmt="%Y-%m-%d_%H:%M:%S",
                        filename=filename_full_path,
                        filemode="a")

#        fileHandler = logging.FileHandler(filename=filename_full_path,
#                                          mode="a")
#        fileHandlerFormat = logging.Formatter(
#            datefmt="%Y-%m-%d_%H:%M:%S,
#            fmt=("[%(asctime)s] "
#                 "[PID %(process)d] "
#                 "[%(filename)s] "
#                 "[%(module)s:%(funcName)s] "
#                 "[%(name)s] "
#                 "[%(levelname)s] "
#                 "%(message)s"))
#        fileHandler.setFormatter(fileHandlerFormat)
#        fileHandler.setLevel(logging_level)
#        logging.getLogger("").addHandler(fileHandler)

    # log info to stdout, display messages with different format than the
    # file output
    if onscreen_log_level:
        onscreen_log_level_lower = onscreen_log_level.lower()
        if (onscreen_log_level_lower in ["debug", "info", "warning",
                                         "error", "critical"]):

            console = logging.StreamHandler()
            screen_handler_format = (
                logging.Formatter(datefmt="%Y-%m-%d_%H:%M:%S",
                                  fmt="[%(asctime)s] > %(message)s"))

            if onscreen_log_level_lower == "debug":
                screen_logging_level = logging.DEBUG
                console.setLevel(screen_logging_level)

                screen_handler_format = (
                    logging.Formatter(datefmt="%Y-%m-%d_%H:%M:%S",
                                      fmt=("[%(asctime)s] > "
                                           "[%(filename)s:%(lineno)d] "
                                           "%(message)s")))

                if not verbose:
                    logging.error("Logging on Screen: Option DEBUG in only "
                                  "active when using verbose option as well "
                                  "(Fallback to INFO).")
            elif onscreen_log_level_lower == "info":
                screen_logging_level = logging.INFO
                console.setLevel(screen_logging_level)
            elif onscreen_log_level_lower == "warning":
                screen_logging_level = logging.WARNING
                console.setLevel(screen_logging_level)
            elif onscreen_log_level_lower == "error":
                screen_logging_level = logging.ERROR
                console.setLevel(screen_logging_level)
            elif onscreen_log_level_lower == "critical":
                screen_logging_level = logging.CRITICAL
                console.setLevel(screen_logging_level)

            console.setFormatter(screen_handler_format)
            logging.getLogger("").addHandler(console)
        else:
            logging.error("Logging on Screen: Option {0} is not supported."
                          .format(onscreen_log_level))
