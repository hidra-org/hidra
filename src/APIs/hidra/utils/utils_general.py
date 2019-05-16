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

import errno
from importlib import import_module
import logging
import os
import platform
import pwd
import socket as socket_m
import sys

from ._version import __version__
from .utils_datatypes import WrongConfiguration


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
    except ModuleNotFoundError:
        logging.error("Module '%s' could not be loaded.", m_type)
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
        # check directory path for existance. exits if it does not exist
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


def check_existance(path):
    """Checks if a file or directory exists. Exists otherwise.

    Args:
        path (str): Absolute path of the directory or file.

    Raises:
        WrongConfigruation: when file or dir is not set or does not exist.
    """

    if path is None:
        raise WrongConfiguration("No path to check found (path={}). Abort."
                                 .format(path))

    # Check path for existance.
    # Exits if it does not exist
    if os.path.isdir(path):
        obj_type = "Dir"
    else:
        obj_type = "File"

    if not os.path.exists(path):
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


def check_version(version, log):
    """ Compares version depending on the minor releases.

    Args:
        version (str): version string of the form
                       <major release>.<minor release>.<patch level>
        log: logging handler
    """

    v_remote = version.rsplit(".", 1)[0]
    v_local = __version__.rsplit(".", 1)[0]
    log.debug("remote version: %s, local version: %s", version, __version__)

    if v_remote < v_local:
        log.info("Version of receiver is lower. Please update receiver.")
        return False
    elif v_remote > v_local:
        log.info("Version of receiver is higher. Please update sender.")
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
    # existance of mount point/monitored dir is essential to start at all
    check_existance(dir_path)

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

        os.seteuid(user_info.pw_uid)
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
        uis_changed_flag: flag if the change
        user_info: a password database entry of the user name changed to.
    """

    if user_was_changed:
        log.info("Running as user %s (uid %s)",
                 user_info.pw_name, user_info.pw_uid)
    elif is_windows():
        log.info("No user change performed (windows), "
                 "running as user %s, (uid %s)",
                 user_info.pw_name, user_info.pw_uid)
    else:
        log.info("No user change needed, running as user %s (uid %s)",
                 user_info.pw_name, user_info.pw_uid)


def show_files_in_dir(log, dirs):
    """Checks if a directory is empty or if not, shows all contained files.

    Args:
        log: log handler
        dirs (list): The directories to check.
    """

    files = []

    for i in dirs:
        files += [
            os.path.join(root, f)
            for root, _, files in os.walk(i)
            for f in files
        ]

    if files:
        log.debug("Files remaining: %s", files)
    else:
        log.debug("No files remaining.")
