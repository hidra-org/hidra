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
This module provides utilities use throughout different parts of hidra.
"""

from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import copy
import errno
from importlib import import_module
import logging
import os
import platform
import re
import socket as socket_m
import subprocess
import sys
import tempfile

try:
    import pwd
except ImportError:
    # on windows
    pass

try:
    # python3
    from pathlib import Path
except ImportError:
    # python2
    from pathlib2 import Path

from ._version import __version__
from .utils_datatypes import WrongConfiguration, NotFoundError, NotSupported


def is_windows():
    """Determines if code is run on a windows system.

    Returns:
        True if on windows, False otherwise.
    """

    return platform.system() == "Windows"


def is_linux():
    """Determines if code is run on a Linux system.

    Returns:
        True if on linux, False otherwise.
    """

    return platform.system() == "Linux"


def check_module_exist(m_type):
    """Checks if the module is available. Exits program if not.

    Args:
        m_type (str): the module to check for existence.
    """

    try:
        import_module(m_type)
        logging.debug("Module '%s' is ok.", m_type)
    except (NotFoundError, ImportError):
        #  This will also be caught if the module is found but one dependency
        #  inside the module is not satisfied
        logging.error("Module '%s' could not be loaded.", m_type,
                      exc_info=True)
        sys.exit(1)


def check_type(specified_type, supported_types, log_string):
    """Checks if type is of the correct form. Exits program if not.

    Args:
        specified_type (str): The type to check.
        supported_types (list): The supported types to check against.
        log_string (str): String to start the log message with.
    """

    specified_type = specified_type.lower()

    if specified_type in supported_types:
        logging.debug("%s '%s' is ok.", log_string, specified_type)
    else:
        logging.error("%s '%s' is not supported.", log_string, specified_type)
        sys.exit(1)


def check_any_sub_dir_exists(dir_path, subdirs):
    """
    Checks if directory contains any the subdirs. Exits program if none exists.

    Args:
        dir_path (str): Absolute path of the directory to check.
        subdirs (list): List of subdirectories to check.
    """

    dir_path = os.path.normpath(dir_path)
    dirs_to_check = [os.path.join(dir_path, directory)
                     for directory in subdirs]
    no_subdir = True

    for i in dirs_to_check:
        # check directory path for existence. exits if it does not exist
        if os.path.exists(i):
            no_subdir = False

    if no_subdir:
        logging.error("There are none of the specified subdirectories inside "
                      "'%s'. Abort.", dir_path)
        logging.error("Checked paths: %s", dirs_to_check)
        sys.exit(1)


def check_sub_dir_contained(dir_path, subdirs):
    """
    Checks if dir contains one of the subdirs.
    e.g. dir_path=/gpfs, subdirs=[current/raw] -> False
         dir_path=/beamline/p01/current/raw, subdirs=[current/raw] -> True

    Args:
        dir_path (str): Absolute path of the directory to check.
        subdirs (list): List of subdirectories to check.

    Returns:
        True if the subdirs are contained, False otherwise.
    """

    subdir_contained = False
    for subdir in subdirs:
        if dir_path[-len(subdir):] == subdir:
            subdir_contained = True

    return subdir_contained


def check_all_sub_dir_exist(dir_path, subdirs):
    """Checks that all subdirecories exist. Exits otherwise.

    Args:
        dir_path (str): Absolute path of the directory to check.
        subdirs (list): List of subdirectories to check.
    """

    dir_path = os.path.normpath(dir_path)
    dirs_to_check = [os.path.join(dir_path, directory)
                     for directory in subdirs]

    for i in dirs_to_check:
        if not os.path.exists(i):
            logging.error("Dir '%s' does not exist. Abort.", i)
            sys.exit(1)


def check_existence(path):
    """Checks if a file or directory exists. Exists otherwise.

    Args:
        path (Path or str): Absolute path of the directory or file.

    Raises:
        WrongConfiguration: when file or dir is not set or does not exist.
    """

    if path is None:
        raise WrongConfiguration("No path to check found (path={}). Abort."
                                 .format(path))

    path = Path(path)

    # Check path for existence.
    # Exits if it does not exist
    if path.is_dir():
        obj_type = "Dir"
    else:
        obj_type = "File"

    if not path.exists():
        raise WrongConfiguration("{} '{}' does not exist. Abort."
                                 .format(obj_type, path))


def check_writable(file_to_check):
    """ Check if hte file can be written. Exists otherwise.

    Args:
        file_to_check (str): Absolute path of the file to check.
    """
    try:
        file_descriptor = open(file_to_check, "a")
        file_descriptor.close()
    except Exception:
        logging.error("Unable to create the file %s", file_to_check)
        sys.exit(1)


def check_version(version, log, check_minor=False):
    """ Compares version depending on the major releases.

    Args:
        version (str): version string of the form
            <major release>.<minor release>.<patch level>
        log: logging handler
        check_minor: Instead of checking the major release, check for minor
            ones
    """
    log.debug("remote version: %s, local version: %s", version, __version__)

    if check_minor:
        check_index = 1
    else:
        check_index = 2

    v_remote = version.rsplit(".", check_index)[0]
    v_local = __version__.rsplit(".", check_index)[0]

    if v_remote < v_local:
        log.info("Remote version is lower. Please update remote version.")
        return False
    elif v_remote > v_local:
        log.info("Remote version is higher. Please update your local version.")
        return False
    else:
        return True


def check_host(host, whitelist, log):
    """Checks if a host is allowed to connect.

    Args:
        host: The host to check.
        whitelist: The whitelist to check against.
        log: log handler.

    Returns:
        A boolean of the result.
    """

    if whitelist is None:
        return True

    if host and whitelist:
        if isinstance(host, list):
            return_val = True
            for hostname in host:
                host_modified = socket_m.getfqdn(hostname)

                if host_modified not in whitelist:
                    log.info("Host %s is not allowed to connect", hostname)
                    return_val = False

            return return_val

        else:
            host_modified = socket_m.getfqdn(host)

            if host_modified in whitelist:
                return True
            else:
                log.info("Host %s is not allowed to connect", host)

    return False


def check_ping(host, log=logging):
    """Check if a host is pingable. Exists if not.

    Args:
        host: The host to check.
        log (optional): log handler.
    """

    if is_windows():
        response = os.system("ping -n 1 -w 2 {}".format(host))
    else:
        response = os.system("ping -c 1 -w 2 {} > /dev/null 2>&1"
                             .format(host))

    if response != 0:
        log.error("%s is not pingable.", host)
        sys.exit(1)


def create_dir(directory, chmod=None, log=logging):
    """Creates the directory if it does not exist.

    Args:
        directory: The absolute path of the directory to be created.
        chmod (optional): Mode bits to change the permissions of the directory
                          to.
        log (optional): log handler.
    """

    if not os.path.isdir(directory):
        os.mkdir(directory)
        log.info("Creating directory: %s", directory)

    if chmod is not None:
        # the permission have to changed explicitly because
        # on some platform they are ignored when called within mkdir
        os.chmod(directory, 0o777)


def create_sub_dirs(dir_path, subdirs, dirs_not_to_create=()):
    """
    Create subdirectories while making sure that certain dirs are not created.
    e.g. current/raw/my_dir should be created but without creating current/raw

    Args:
        dir_path (str): Absolute path of the base where for all
                        subdirectories.
        subdirs (list): The subdirectories to create.
        dirs_not_to_create: The directories to make sure not to create by
                            accident.

    Raises:
        OSError: If directory create failed.
    """

    dir_path = os.path.normpath(dir_path)
    # existence of mount point/monitored dir is essential to start at all
    check_existence(dir_path)

    dirs_not_to_create = tuple(dirs_not_to_create)
    dirs_to_check = [os.path.join(dir_path, directory)
                     for directory in subdirs
                     if not directory.startswith(dirs_not_to_create)]

    throw_exception = False
    for i in dirs_to_check:
        try:
            os.makedirs(i)
            logging.debug("Dir '%s' does not exist. Create it.", i)
        except OSError as excp:
            if excp.errno == errno.EEXIST:
                # dir exists already
                pass
            else:
                logging.error("Dir '%s' could not be created.", i)
                throw_exception = True
                raise

    if throw_exception:
        raise OSError


class TempFile:
    def __init__(self, temp_file, target_path):
        """
        Wraps a temporary file and renames it to the given path on close

        Parameters
        ----------
        temp_file: file object
            An open, temporary file object
        target_path: str or Path-like
            The final target path the file will have after calling close
        """
        self.temp_file = temp_file
        self.target_path = str(target_path)

    def seek(self, offset, whence=0):
        return self.temp_file.seek(offset, whence)

    def truncate(self, size=None):
        return self.temp_file.truncate(size)

    def write(self, data):
        return self.temp_file.write(data)

    def close(self):
        if not self.temp_file.closed:
            self.temp_file.close()
            os.chmod(self.temp_file.name, 0o644)
            os.rename(self.temp_file.name, self.target_path)


def open_tempfile(filepath):
    """
    Open a temporary file that will be automatically renamed to filepath when
    closed.

    The file is opened in 'w+b' mode but only the write, seek, truncate, and
    close methods are supported by the returned object.
    """

    filepath = Path(filepath)
    f = tempfile.NamedTemporaryFile(
        suffix='.tmp', prefix=filepath.name + '.', dir=str(filepath.parent),
        delete=False)
    return TempFile(f, filepath)


def change_user(config):
    """Set the effective uid to the username.

    Args:
        config (dict): A configuration dictionary containing the username as
                       entry "username".

    Returns:
        A tuple containing the password database entry of the effective user
        and a boolean if the user was changed or just stayed as is
        (True: was changed).
    """

    if is_windows():
        if "username" in config:
            # this is not working on windows
            return config["username"], False
        else:
            return None, False

    start_user = pwd.getpwuid(os.geteuid())

    try:
        if start_user.pw_name == config["username"]:
            # nothing to do
            return start_user, False
    except KeyError:
        # no user change needed
        return start_user, False

    try:
        # get uid as int
        user_info = pwd.getpwnam(config["username"])

        # set supplemental gids
        os.initgroups(config["username"], user_info.pw_gid)

        os.setgid(user_info.pw_gid)
        os.setuid(user_info.pw_uid)
    except AttributeError:
        # on windows (user change is not possible)
        user_info = start_user
    except Exception:
        logging.error("Failed to set user to %s", config["username"])
        raise

    return user_info, True


def log_user_change(log, user_was_changed, user_info):
    """Logs if a user change took place

    Args:
        log: log handler
        user_was_changed: flag if the change
        user_info: a password database entry of the user name changed to.
    """

    if is_windows():
        if user_info is not None:
            log.info("No user change performed (Windows)")
        # otherwise no output is needed
        return

    if user_was_changed:
        log.info("Running as user %s (uid %s, gid %s)",
                 user_info.pw_name, user_info.pw_uid, user_info.pw_gid)
    else:
        log.info("No user change needed, running as user %s (uid %s, gid %s)",
                 user_info.pw_name, user_info.pw_uid, user_info.pw_gid)


def get_files_in_dir(dirs):
    """Checks if a directory is empty or if not, shows all contained files.

    Args:
        dirs (list): The directories to check.
    """

    files = []

    for i in dirs:
        files += [
            os.path.join(root, f)
            for root, _, files in os.walk(i)
            for f in files
        ]

    return files


def get_service_manager(systemd_prefix, service_name):
    """
    Determines which kind of service manager system to use (systemd or init).

    Return:
        A string if service manager lookup was successful:
            "systemd": on a modern linux system using systemd
            "init": on a older system using init scripts
        And None if it could not be determined.
    """

    path_to_check = [
        "/usr/lib/systemd",
        "/usr/lib/systemd/system",
        "/etc/systemd/system",
        "/lib/systemd/system"
    ]

    use_systemd = (
        os.path.exists("/usr/lib/systemd")
        and any([os.path.exists("{}/{}.service".format(i, systemd_prefix))
                 for i in path_to_check])
    )

    use_init_script = (
        os.path.exists("/etc/init.d")
        and os.path.exists("/etc/init.d/" + service_name)
    )

    if use_systemd:
        service_manager = "systemd"
    elif use_init_script:
        service_manager = "init"
    else:
        service_manager = None

    return service_manager


def read_status(service, log, service_manager="systemd"):
    """
    Get more status information. Only available for systems using systemd.

    Args:
        service_manager: The kind of service manager the system uses
                         (systemd or init).
        service: The service name prefix (for systemd e.g hidra@)
        log: Logging handle

    Returns:
        A dictionary with the detail information:
            service
            status
            since
            uptime
            pid
            info
    """

    if service_manager == "systemd":
        return _read_status_systemd(service, log)
    elif service_manager == "init":
        return _read_status_init(service, log)
    else:
        raise NotSupported("Service manager '{}' is not supported."
                           .format(service_manager))


def _read_status_systemd(service, log):
    """
    Get more status information. Only available for systems using systemd.

    Args:
        service: The systemd service name prefix (e.g hidra@)
        log: Logging handle

    Returns:
        A dictionary with the detail information:
            service
            status
            since
            uptime
            pid
            info
    """

    cmd = ["systemctl", "show", service]

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        (output, _) = proc.communicate()
        output = output.decode('utf-8')
    except Exception:
        log.error("Error when calling systemctl", exc_info=True)
        raise

    # TODO check err
    # TODO check: return_val = systemctl.returncode

    service_regex = r"Loaded:.*\/(.*service);"
    status_regex = r"Active:(.*) since (.*);(.*)"
    pid_regex = r"MainPID=([0-9]*)"
    message_regex = r".*datamanager.py\[[0-9]*\]:(.*)"

    service_status = {
        "service": None,
        "status": None,
        "since": None,
        "uptime": None,
        "pid": None,
        "info": None,
    }

    for line in output.splitlines():
        service_search = re.search(service_regex, line)
        status_search = re.search(status_regex, line)
        pid_search = re.search(pid_regex, line)
        message_search = re.search(message_regex, line)

        if service_search:
            service_status["service"] = service_search.group(1)

        elif status_search:
            service_status["status"] = status_search.group(1).strip()
            service_status["since"] = status_search.group(2).strip()
            service_status["uptime"] = status_search.group(3).strip()

        elif pid_search:
            service_status["pid"] = pid_search.group(1)

        elif message_search:
            if service_status["info"] is None:
                service_status["info"] = message_search.group(1)
            else:
                service_status["info"] += "\n" + message_search.group(1)

    return service_status


def _read_status_init(service, log):  # pylint: disable=unused-argument
    """
    Args:
        service: The systemd service name prefix (e.g hidra@)
        log: Logging handle

    Returns:
        A dictionary with the detail information:
            service
            status
            since
            uptime
            pid
            info
    """

    raise NotSupported("Using init script is not implemented (yet)!")
    # pylint: disable=unreachable

#    cmd = ["service", service, "status"]
#
#    try:
#        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
#        (output, err) = proc.communicate()
#        output = output.decode('utf-8')
#    except Exception:
#        log.debug("systemctl error was: %s", err)
#        raise
#
#    service_status = {
#        "service": None,
#        "status": None,
#        "since": None,
#        "uptime": None,
#        "pid": None,
#        "info": None,
#    }
#
#    return service_status


def get_by_path(root, items):
    """
    Access a nested object in root by a items sequence.

    Args:
        root: The object (e.g. dictionary) to get values from.
        items: The list of keys used to reach the requested leaf.

    Returns:
        The value of the leaf.
    """

    tmp = copy.deepcopy(root)
    for k in items:
        tmp = tmp[k]

    return tmp


def set_by_path(root, items, value):
    """
    Set a value in a nested object in root by item sequence.

    Args:
        root: The object (e.g. dictionary) to set values to
        items:The list of keys used to reach the requested leaf.
        value: The value to set the leaf to.
    """
    get_by_path(root, items[:-1])[items[-1]] = value
