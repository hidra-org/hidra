from __future__ import unicode_literals
from __future__ import print_function

import errno
import json
import logging
import logging.handlers
import os
import platform
import re
import shutil
import subprocess
import socket
import sys
from collections import namedtuple

from cfel_optarg import parse_parameters

from _version import __version__
from hidra import LoggingFunction

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
    from shared import SHARED_DIR
    if SHARED_DIR not in sys.path:
        sys.path.insert(0, SHARED_DIR)

    from logutils.queue import QueueListener
    from logutils.queue import QueueHandler


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


class WrongConfiguration(Exception):
    pass


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


def set_parameters(base_config_file, config_file, arguments):
    base_config = parse_parameters(read_config(base_config_file))["asection"]

    if config_file is not None:
        config = parse_parameters(read_config(config_file))["asection"]

        # overwrite base config parameters with the ones in the config_file
        for key in config:
            base_config[key] = config[key]

    # arguments set when the program is called have a higher priority than
    # the ones in the config file
    for arg in vars(arguments):
        arg_value = getattr(arguments, arg)
        if arg_value is not None:
            if type(arg_value) is str:
                if arg_value.lower() == "none":
                    base_config[arg] = None
                elif arg_value.lower() == "false":
                    base_config[arg] = False
                elif arg_value.lower() == "true":
                    base_config[arg] = True
                else:
                    base_config[arg] = arg_value
            else:
                base_config[arg] = arg_value

    return base_config


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
            ans = raw_input(prompt)  # noqa F821
        except KeyboardInterrupt:
            logging.error("Keyboard Interruption detected.")
        except Exception as e:
            logging.error("Something went wrong with the confirmation.")
            logging.debug("Error was: {}".format(e))
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
        logging.debug("{} '{}' is ok.".format(log_string, specified_type))
    else:
        logging.error("{} '{}' is not supported."
                      .format(log_string, specified_type))
        sys.exit(1)


def check_dir_empty(dir_path):

    # check if directory is empty
    if os.listdir(dir_path):
        msg = "Directory '{}' is not empty.".format(dir_path)
        logging.debug(msg)
        prompt = msg + "\nShould its content be removed?"
        if confirm(prompt=prompt, resp=True):
            for element in os.listdir(dir_path):
                path = os.path.join(dir_path, element)
                if os.path.isdir(path):
                    try:
                        os.rmdir(path)
                    except OSError:
                        shutil.rmtree(path)
                else:
                    os.remove(path)
            logging.info("All elements of directory {} were removed."
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
                      "'{}'. Abort.".format(dir_path))
        logging.error("Checked paths: {}".format(dirs_to_check))
        sys.exit(1)


def check_sub_dir_contained(dir_path, subdirs):
    """ Checks for dir_path contains one of the subdirs
        e.g. dir_path=/gpfs, subdirs=[current/raw] -> False
             dir_path=/beamline/p01/current/raw, subdirs=[current/raw] -> True
    """
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
            logging.error("Dir '{}' does not exist. Abort.".format(d))
            sys.exit(1)


def check_existance(path):
    if path is None:
        logging.error("No path to check found (path={}). Abort.".format(path))
        sys.exit(1)

    # Check path for existance.
    # Exits if it does not exist
    if os.path.isdir(path):
        obj_type = "Dir"
    else:
        obj_type = "File"

    if not os.path.exists(path):
        logging.error("{} '{}' does not exist. Abort."
                      .format(obj_type, path))
        sys.exit(1)


def check_writable(file_to_check):
    # Exits if file can be written
    try:
        file_descriptor = open(file_to_check, "a")
        file_descriptor.close()
    except:
        logging.error("Unable to create the file {}".format(file_to_check))
        sys.exit(1)


def check_version(version, log):
    """ Compares version depending on the minor releases

    Args:
        version (str): version string of the form
                       <major release>.<minor release>.<patch level>
        log: logging handler
    """
    log.debug("remote version: {}, local version: {}"
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

    if whitelist is None:
        return True

    if host and whitelist:
        if type(host) == list:
            return_val = True
            for hostname in host:
                host_modified = socket.getfqdn(hostname)

                if (host_modified not in whitelist):
                    log.info("Host {} is not allowed to connect"
                             .format(hostname))
                    return_val = False

            return return_val

        else:
            host_modified = socket.getfqdn(host)

            if host_modified in whitelist:
                return True
            else:
                log.info("Host {} is not allowed to connect".format(host))

    return False


def check_ping(host, log=logging):
    if is_windows():
        response = os.system("ping -n 1 -w 2 {}".format(host))
    else:
        response = os.system("ping -c 1 -w 2 {} > /dev/null 2>&1"
                             .format(host))

    if response != 0:
        log.error("{} is not pingable.".format(host))
        sys.exit(1)


def create_dir(directory, chmod=None, log=logging):
    """Creates the directory if it does not exist.

    Args:
        directory: The absolute path of the directory to be created.
        chmod (optional): Mode bits to change the permissions of the directory
                          to.
    """

    if not os.path.isdir(directory):
        os.mkdir(directory)
        log.info("Creating directory: {}".format(directory))

    if chmod is not None:
        # the permission have to changed explicitly because
        # on some platform they are ignored when called within mkdir
        os.chmod(directory, 0o777)


def create_sub_dirs(dir_path, subdirs, dirs_not_to_create=()):

    dir_path = os.path.normpath(dir_path)
    # existance of mount point/monitored dir is essential to start at all
    check_existance(dir_path)

    dirs_not_to_create = tuple(dirs_not_to_create)
    dirs_to_check = [os.path.join(dir_path, directory)
                     for directory in subdirs
                     if not directory.startswith(dirs_not_to_create)]

    throw_exception = False
    for d in dirs_to_check:
        try:
            os.makedirs(d)
            logging.debug("Dir '{}' does not exist. Create it.".format(d))
        except OSError as e:
            if e.errno == errno.EEXIST:
                # file exists already
                pass
            else:
                logging.error("Dir '{}' could not be created.".format(d))
                throw_exception = True
                raise

    if throw_exception:
        raise OSError


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
        config_reduced (str): serialized dict containing the values of the
                              required parameters only
    """

    check_passed = True
    config_reduced = {}

    for param in required_params:
        # multiple checks have to be done
        if type(param) == list:
            # checks if the parameter is contained in the config dict
            if param[0] not in config:
                log.error("Configuration of wrong format. "
                          "Missing parameter '{}'".format(param[0]))
                check_passed = False
            # check if the parameter is one of the supported values
            elif type(param[1]) == list:
                if config[param[0]] not in param[1]:
                    log.error("Configuration of wrong format. Options for "
                              "parameter '{}' are {}"
                              .format(param[0], param[1]))
                    log.debug("parameter '{}' = {}"
                              .format(param[0], config[param[0]]))
                    check_passed = False
            # check if the parameter has the supported type
            elif type(config[param[0]]) != param[1]:
                log.error("Configuration of wrong format. Parameter '{}' is "
                          "of format '{}' but should be of format '{}'"
                          .format(param[0], type(config[param[0]]), param[1]))
                check_passed = False
        # checks if the parameter is contained in the config dict
        elif param not in config:
            log.error("Configuration of wrong format. Missing parameter: '{}'"
                      .format(param))
            check_passed = False
        else:
            config_reduced[param] = config[param]

    try:
        dict_to_str = str(
            json.dumps(config_reduced, sort_keys=True, indent=4)
        )
    except TypeError:
        # objects like e.g. zm.context are not JSON serializable
        # convert manually
        sorted_keys = sorted(config_reduced.keys())
        indent = 4

        # putting it into a list first and the join it if more efficient
        # than string concatenation
        dict_to_list = []
        for key in sorted_keys:
            value = config_reduced[key]
            if type(value) == Endpoints:
                new_value = json.dumps(value._asdict(),
                                       sort_keys=True,
                                       indent=2 * 4)
                as_str = "{}{}: {}".format(" " * indent, key, new_value)
                # fix indentation
                as_str = as_str[:-1] + " " * indent + "}"
            else:
                as_str = "{}{}: {}".format(" " * indent, key, value)

            dict_to_list.append(as_str)

        dict_to_str = "{\n"
        dict_to_str += ",\n".join(dict_to_list)
        dict_to_str += "\n}"

    return check_passed, dict_to_str


def extend_whitelist(whitelist, ldapuri, log):
    """
    Only fully qualified domain named should be in the whitlist
    """

    log.info("Configured whitelist: {}".format(whitelist))

    if whitelist is not None:
        if type(whitelist) == str:
            whitelist = excecute_ldapsearch(whitelist, ldapuri)
            log.info("Whitelist after ldapsearch: {}".format(whitelist))
        else:
            whitelist = [socket.getfqdn(host) for host in whitelist]
            log.debug("Converted whitelist: {}".format(whitelist))

    return whitelist


def convert_socket_to_fqdn(socketids, log):
    """
    Converts hosts to fully qualified domain name
    e.g. [["my_host:50101", ...], ...] -> [["my_host.desy.de:50101", ...], ...]
    or "my_host:50101" -> "my_host.desy.de:50101"
    """
    if type(socketids) == list:
        for target in socketids:
            # socketids had the format
            # [["cfeld-pcx27533:50101", 1, ".*(tif|cbf)$"], ...]
            if type(target) == list:
                host, port = target[0].split(":")
                new_target = "{}:{}".format(socket.getfqdn(host), port)
                target[0] = new_target
    else:
        host, port = socketids.split(":")
        socketids = "{}:{}".format(socket.getfqdn(host), port)

    log.debug("converted socketids={}".format(socketids))

    return socketids


def is_ipv6_address(log, ip):
    """" Determines if given IP is an IPv4 or an IPv6 addresses

    Args:
        log: logger for the log messages
        ip: IP address to check

    Returns:
        boolean notifying if the IP was IPv4 or IPv6
    """
    try:
        socket.inet_aton(ip)
        log.info("IPv4 address detected: {}.".format(ip))
        return False
    except socket.error:
        log.info("Address '{}' is not an IPv4 address, asume it is an IPv6 "
                 "address.".format(ip))
        return True


def get_socket_id(log, ip, is_ipv6=None):
    """ Determines socket ID for the given port

    If the IP is an IPV6 address the appropriate zeromq syntax is used.

    Args:
        log: logger for the log messages
        ip: socket ip
        is_ipv6 (bool or None, optional): using the IPv6 syntax. If not set,
                                          the type of the IP is determined
                                          first.

    """

    if is_ipv6 is None:
        is_ipv6 = is_ipv6_address(log, ip)

    if is_ipv6:
        return "[{}]:{}".format(ip)
    else:
        return "{}:{}".format(ip)


def excecute_ldapsearch(ldap_cn, ldapuri):

    p = subprocess.Popen(
        ["ldapsearch",
         "-x",
         "-H ldap://" + ldapuri,
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


def generate_sender_id(main_pid):
    """ Generates an unique id to identify the running datamanager.

    Args:
        main_pid: The PID of the datamanager
    Return:
        A byte string containing the identifier
    """

    return b"{}_{}".format(socket.getfqdn(), main_pid)


# ------------------------------ #
#  Connection paths and strings  #
# ------------------------------ #

# To be pickable these have to be defined at the top level of a module
# this is needed because multiprocessing on windows needs these pickable.
# Additionally the name of the namedtuple has to be the same as the typename
# otherwise it cannot be pickled on Windows.


IpcAddresses = namedtuple(
    "IpcAddresses", [
        "control_pub",
        "control_sub",
        "request_fw",
        "router",
        "cleaner_job",
        "cleaner_trigger",
    ]
)


def set_ipc_addresses(ipc_dir, main_pid, use_cleaner=True):
    """Sets the ipc connection paths.

    Sets the connection strings  for the job, control, trigger and
    confirmation socket.

    Args:
        ipc_dir: Directory used for IPC connections
        main_pid: Process ID of the current process. Used to distinguish
                  different IPC connection.
    Returns:
        A namedtuple object IpcAddresses with the entries:
            control_pub,
            control_sub,
            request_fw,
            router,
            cleaner_job,
            cleaner_trigger
    """

    # determine socket connection strings
    ipc_ip = "{}/{}".format(ipc_dir, main_pid)

    control_pub = "{}_{}".format(ipc_ip, "controlPub")
    control_sub = "{}_{}".format(ipc_ip, "controlSub")
#    control_pub = "{}_{}".format(ipc_ip, "control_pub")
#    control_sub = "{}_{}".format(ipc_ip, "control_sub")
    request_fw = "{}_{}".format(ipc_ip, "requestFw")
#    request_fw = "{}_{}".format(ipc_ip, "request_fw")
    router = "{}_{}".format(ipc_ip, "router")

    if use_cleaner:
        job = "{}_{}".format(ipc_ip, "cleaner")
        trigger = "{}_{}".format(ipc_ip, "cleaner_trigger")
    else:
        job = None
        trigger = None

    return IpcAddresses(
        control_pub=control_pub,
        control_sub=control_sub,
        request_fw=request_fw,
        router=router,
        cleaner_job=job,
        cleaner_trigger=trigger,
    )


Endpoints = namedtuple(
    "Endpoints", [
        "control_pub_bind",
        "control_pub_con",
        "control_sub_bind",
        "control_sub_con",
        "request_bind",
        "request_con",
        "request_fw_bind",
        "request_fw_con",
        "router_bind",
        "router_con",
        "com_bind",
        "com_con",
        "cleaner_job_bind",
        "cleaner_job_con",
        "cleaner_trigger_bind",
        "cleaner_trigger_con",
        "confirm_bind",
        "confirm_con",
    ]
)


def set_endpoints(ext_ip, con_ip, ports, ipc_addresses, use_cleaner=True):
    """Configures the ZMQ address depending on the protocol.

    Sets the connection strings  for the job, control, trigger and
    confirmation socket.

    Args:
        ext_ip: IP to bind TCP connections to
        con_ip: IP to connect TCP connections to
        ipc_addresses: Addresses to use for the IPC connections.
        port: A dictionary giving the ports to open TCP connection on
              (only used on Windows).
    Returns:
        A namedtuple object Endpoints with the entries:
            control_pub_bind
            control_pub_con
            control_sub_bind
            control_sub_con
            request_bind,
            request_con,
            request_fw_bind,
            request_fw_con,
            router_bind,
            router_con,
            com_bind
            com_con
            cleaner_job_bind
            cleaner_job_con
            cleaner_trigger_bind
            cleaner_trigger_con
            confirm_bind
            confirm_con
    """

    # determine socket connection strings
    if is_windows():
        port = ports["control_pub"]
        control_pub_bind = "tcp://{}:{}".format(ext_ip, port)
        control_pub_con = "tcp://{}:{}".format(con_ip, port)

        port = ports["control_sub"]
        control_sub_bind = "tcp://{}:{}".format(ext_ip, port)
        control_sub_con = "tcp://{}:{}".format(con_ip, port)

        port = ports["request_fw"]
        request_fw_bind = "tcp://{}:{}".format(ext_ip, port)
        request_fw_con = "tcp://{}:{}".format(con_ip, port)

        port = ports["router"]
        router_bind = "tcp://{}:{}".format(ext_ip, port)
        router_con = "tcp://{}:{}".format(con_ip, port)

    else:
        control_pub_bind = "ipc://{}".format(ipc_addresses.control_pub)
        control_pub_con = control_pub_bind

        control_sub_bind = "ipc://{}".format(ipc_addresses.control_sub)
        control_sub_con = control_sub_bind

        request_fw_bind = "ipc://{}".format(ipc_addresses.request_fw)
        request_fw_con = request_fw_bind

        router_bind = "ipc://{}".format(ipc_addresses.router)
        router_con = router_bind

    request_bind = "tcp://{}:{}".format(ext_ip, ports["request"])
    request_con = "tcp://{}:{}".format(con_ip, ports["request"])

    com_bind = "tcp://{}:{}".format(ext_ip, ports["com"])
    com_con = "tcp://{}:{}".format(con_ip, ports["com"])

    # endpoints needed if cleaner is activated
    if use_cleaner:
        if is_windows():
            port = ports["cleaner"]
            job_bind = "tcp://{}:{}".format(ext_ip, port)
            job_con = "tcp://{}:{}".format(con_ip, port)

            port = ports["cleaner_trigger"]
            trigger_bind = "tcp://{}:{}".format(ext_ip, port)
            trigger_con = "tcp://{}:{}".format(con_ip, port)

        else:
            job_bind = "ipc://{}".format(ipc_addresses.cleaner_job)
            job_con = job_bind

            trigger_bind = "ipc://{}".format(ipc_addresses.cleaner_trigger)
            trigger_con = trigger_bind

        # use self.params["data_stream_targets"][0][0] instead of
        # ext_ip, con_ip
        confirm_bind = "tcp://{}:{}".format(ext_ip, ports["confirmation"])
        confirm_con = "tcp://{}:{}".format(con_ip, ports["confirmation"])
    else:
        job_bind = None
        job_con = None
        trigger_bind = None
        trigger_con = None
        confirm_bind = None
        confirm_con = None

    return Endpoints(
        control_pub_bind=control_pub_bind,
        control_pub_con=control_pub_con,
        control_sub_bind=control_sub_bind,
        control_sub_con=control_sub_con,
        request_bind=request_bind,
        request_con=request_con,
        request_fw_bind=request_fw_bind,
        request_fw_con=request_fw_con,
        router_bind=router_bind,
        router_con=router_con,
        com_bind=com_bind,
        com_con=com_con,
        cleaner_job_bind=job_bind,
        cleaner_job_con=job_con,
        cleaner_trigger_bind=trigger_bind,
        cleaner_trigger_con=trigger_con,
        confirm_bind=confirm_bind,
        confirm_con=confirm_con,
    )


# ------------------------------ #
#         ZMQ functions          #
# ------------------------------ #

MAPPING_ZMQ_CONSTANTS_TO_STR = [
    "PAIR",  # zmq.PAIR = 0
    "PUB",  # zmq.PUB = 1
    "SUB",  # zmq.SUB = 2
    "REQ",  # zmq.REQ = 3
    "REP",  # zmq.REP = 4
    "DEALER/XREQ",  # zmq.DEALER/zmq.XREQ = 5
    "ROUTER/XREP",  # zmq.ROUTER/zmq.XREP = 6
    "PULL",  # zmq.PULL = 7
    "PUSH",  # zmq.PUSH = 8
    "XPUB",  # zmq.XPUB = 9
    "XSUB",  # zmq.XSUB = 10
]


def start_socket(name,
                 sock_type,
                 sock_con,
                 endpoint,
                 context,
                 log,
                 message=None):
    """Creates a zmq socket.

    Args:
        name: The name of the socket (used in log messages).
        sock_type: ZMQ socket type (e.g. zmq.PULL).
        sock_con: ZMQ binding type (connect or bind).
        endpoint: ZMQ endpoint to connect to.
        context: ZMQ context to create the socket on.
        log: Logger used for log messages.
        message (optional): wording to be used in the message
                            (default: Start).
    """

    if message is None:
        message = "Start"

    sock_type_as_str = MAPPING_ZMQ_CONSTANTS_TO_STR[sock_type]

    try:
        socket = context.socket(sock_type)
        if sock_con == "connect":
            socket.connect(endpoint)
        elif sock_con == "bind":
            socket.bind(endpoint)
        log.info("{} {} ({}, {}): '{}'".format(message,
                                               name,
                                               sock_con,
                                               sock_type_as_str,
                                               endpoint))
    except:
        log.error("Failed to {} {} ({}, {}): '{}'".format(name,
                                                          message.lower(),
                                                          sock_con,
                                                          sock_type_as_str,
                                                          endpoint),
                  exc_info=True)
        raise

    return socket


def stop_socket(name, socket, log):
    """Closes a zmq socket.

    Args:
        name: The name of the socket (used in log messages).
        socket: The ZMQ socket to be closed.
        log: Logger used for log messages.

    """

    if socket is not None:
        log.info("Closing {}".format(name))
        socket.close(0)
        socket = None

    return socket


# ------------------------------ #
#            Logging             #
# ------------------------------ #

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
        logging.error("Logging on Screen: Option {} is not supported."
                      .format(loglevel))
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
        loglevel = logging.CRITICAL
    elif loglevel == "error":
        loglevel = logging.ERROR
    elif loglevel == "warning":
        loglevel = logging.WARNING
    elif loglevel == "info":
        loglevel = logging.INFO
    else:
        loglevel = logging.DEBUG

    formatter = logging.Formatter(datefmt=datefmt, fmt=fmt)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(loglevel)

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
        screen_loglevel = onscreen_loglevel.lower()

        if screen_loglevel == "debug":
            if not verbose:
                logging.error("Logging on Screen: Option DEBUG in only "
                              "active when using verbose option as well "
                              "(Fallback to INFO).")

        screen_handler = get_stream_log_handler(loglevel=screen_loglevel)
        return file_handler, screen_handler
    else:
        return file_handler


def get_logger(logger_name, queue=False, log_level="debug"):
    """Send all logs to the main process.

    The worker configuration is done at the start of the worker process run.
    Note that on Windows you can't rely on fork semantics, so each process
    will run the logging configuration code when it starts.
    """
    loglevel = log_level.lower()

    if queue:
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger(logger_name)
        logger.propagate = False
        logger.addHandler(h)

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


def init_logging(filename_full_path, verbose, onscreen_loglevel=False):
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
                        filename=filename_full_path,
                        filemode="a")

#        file_handler = logging.FileHandler(filename=filename_full_path,
#                                           mode="a")
#        file_handler_format = logging.Formatter(datefmt=datefmt,
#                                                fmt=filefmt)
#        file_handler.setFormatter(file_handler_format)
#        file_handler.setLevel(file_log_level)
#        logging.getLogger("").addHandler(file_andler)

    # log info to stdout, display messages with different format than the
    # file output
    if onscreen_loglevel:
        screen_loglevel = onscreen_loglevel.lower()

        if screen_loglevel == "debug" and not verbose:
            logging.error("Logging on Screen: Option DEBUG in only "
                          "active when using verbose option as well "
                          "(Fallback to INFO).")

        screen_handler = get_stream_log_handler(loglevel=screen_loglevel)
        logging.getLogger("").addHandler(screen_handler)
