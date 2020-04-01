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
# Some of configparser specific parts of this file were former members of
# cfelpyutils.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#     Valerio Mariani <valerio.mariani@desy.de>

"""
This module provides utilities regarding config handling.
"""

from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import ast
import copy
import json
import logging
import yaml

from .utils_datatypes import (
    Endpoints,
    NotSupported,
    WrongConfiguration,
    NotFoundError
)
from .utils_general import is_windows
from ._environment import BASE_DIR  # noqa E402

try:
    # python3
    from configparser import RawConfigParser
    from pathlib import Path
except ImportError:
    # python2
    from ConfigParser import RawConfigParser
    from pathlib2 import Path


def get_internal_config_path(filename):
    """Get the configuration path inside default hidra sources

    This function should not be exposed to used but still can be used
    internally (e.g. in control server)

    Args:
        filename: The name of the log file

    Returns:
        A pathlib path object ob the absolute file.
    """
    return Path(BASE_DIR).joinpath("conf", filename)


def _get_config_file_location(filename):
    """ Get the absolute path of the log file.

    For Linux an hierarchical structure is checked:
        - user config path
        - system config path
        - hidra config path
    On windows only the hidra config path is supported.

    Args:
        filename: The name of the log file

    Returns:
        The absolute path where the log file can be found.

    Raises:
        NotFoundError when the log file could not be found in either location.
    """

    if not is_windows():
        # $HOME/.config/hidra/
        user_config_path = Path.home().joinpath(".config/hidra", filename)
        if user_config_path.exists():  # pylint: disable=no-member
            return user_config_path

        # /etc/xdg/hidra
        system_config_path = Path("/etc/xdg/hidra").joinpath(filename)
        if system_config_path.exists():  # pylint: disable=no-member
            return system_config_path

    # /opt/hidra/conf
    hidra_config_path = Path(BASE_DIR).joinpath("conf", filename)
    if hidra_config_path.exists():  # pylint: disable=no-member
        return hidra_config_path
    else:
        raise NotFoundError("Configuration file does not exist.")


def determine_config_file(fname_base):
    """
    Determines the config file location and if it is of conf or yaml type.

    Args:
        fname_base: the file name base of the config file
                    e.g. fname_base for base_sender.yaml would be base_sender
    Returns:
        The base config file (full path).

    Raises:
        WrongConfiguration: if no config was found.
    """

    try:
        conf_file = _get_config_file_location("{}.yaml".format(fname_base))
    except NotFoundError:
        try:
            conf_file = _get_config_file_location("{}.conf".format(fname_base))
        except NotFoundError:
            raise WrongConfiguration("Missing config file ('{}(.yaml|.conf)')"
                                     .format(fname_base))

    return conf_file


def load_config(config_file, config_type=None, log=logging):
    """Read and parse configuration data from the file.

    Args:
        config_file (str or Path): Absolute path to the configuration file.
        config_type (str): The type the configuration is in (config or yaml).
            If not set (or set to None) the file extension is used for
            automatic detection.
        log (optional): A log handler to use.

    Returns:
        Configuration dictionary.
    """
    config_file = Path(config_file)

    # Auto-detection
    if config_type is None:
        file_type = _detect_config_type(config_file, log)
    else:
        file_type = config_type

    try:
        if file_type in [".conf", "conf"]:
            configparser = RawConfigParser()
            try:
                # pylint: disable=deprecated-method
                configparser.readfp(_FakeSecHead(config_file.open('r')))
            # TODO why was this necessary?
            except Exception:
                with config_file.open('r') as f:
                    config_string = '[asection]\n' + f.read()
                configparser.read_string(config_string)

            config = parse_parameters(configparser)["asection"]

        elif file_type in [".yaml", "yaml"]:
            with config_file.open('r') as f:
                config = yaml.safe_load(f)

            # check for "None" entries
            _fix_none_entries(dictionary=config)
        else:
            raise Exception()

    except Exception:
        log.error("Could not load config file %s", config_file)
        raise

    return config


def _detect_config_type(config_file, log):
    file_type = config_file.suffix

    if file_type not in [".yaml", ".conf"]:
        log.debug("config_file = %s", config_file)
        raise WrongConfiguration("Detected not supported config type")

    return file_type


def write_config(config_file, config, config_type=None, log=logging):
    """Write configuration data info a file.

    Args:
        config_file (str or Path): Absolute path to the configuration file.
        config (dict): The configuration data.
        config_type (str, optional): The type the configuration is in
            (config or yaml). If not set (or set to None) the file extension is
            used for automatic detection.
        log (optional): A log handler to use.

    Raises:
        NotSupported if the defined config_type (or when auto-detecting the
        file extension) is not supported.
    """
    config_file = Path(config_file)

    # Auto-detection
    if config_type is None:
        file_type = _detect_config_type(config_file, log)
    else:
        file_type = config_type

    try:
        if file_type in [".conf", "conf"]:
            with config_file.open('w') as f:
                for key, value in config.items():
                    f.write("{} = {}\n".format(key, value))

        if file_type in [".yaml", "yaml"]:
            with config_file.open('w') as outfile:
                yaml.safe_dump(
                    config,
                    outfile,
                    default_flow_style=False,
                )

        else:
            log.debug("config_file=%s", config_file)
            raise NotSupported("Config file type not supported.")
    except Exception:
        log.error("Could not write config file %s", config_file)
        raise


def _fix_none_entries(dictionary):
    """Converts all "None" entries in the dictionary to NoneType.

    Args:
        dictionary (dict): The dictionary to travers.
    """

    for key, value in dictionary.items():
        if isinstance(value, dict):
            _fix_none_entries(value)
        else:
            if value == "None":
                dictionary[key] = None


def update_dict(dictionary, dict_to_update):
    """Updated one dictionary recursively with the entries of another.

    Args:
        dictionary (dict): The dict used to update dict_to_update.
        dict_to_update (dict): The dictionary whose entries should be updated.
    """

    for key, value in dictionary.items():
        if isinstance(value, dict):
            try:
                update_dict(value, dict_to_update[key])
            except KeyError:
                dict_to_update[key] = value
        else:
            dict_to_update[key] = value


CONFIG_MAPPING_SENDER = {
    "general": {
        "log_path": "log_path",
        "log_name": "log_name",
        "log_size": "log_size",
        "username": "username",
        "procname": "procname",
        "ext_ip": "ext_ip",
        "whitelist": "whitelist",
        "com_port": "com_port",
        "ldapuri": "ldapuri",
        "request_port": "request_port",
        "request_fw_port": "request_fw_port",
        "control_pub_port": "control_pub_port",
        "control_sub_port": "control_sub_port",
        "config_file": "config_file",
        "verbose": "verbose",
        "onscreen": "onscreen"
    },
    "eventdetector": {
        "type": "eventdetector_type",
        "ext_data_port": "ext_data_port",
        "eventdetector_port": "eventdetector_port",
        "dirs_not_to_create": "dirs_not_to_create",
        "inotify_events": {
            "monitored_dir": "monitored_dir",
            "fix_subdirs": "fix_subdirs",
            "create_fix_subdirs": "create_fix_subdirs",
            "monitored_events": "monitored_events",
            "history_size": "history_size",
            "use_cleanup": "use_cleanup",
            "action_time": "action_time",
            "time_till_closed": "time_till_closed"
        },
        "inotifyx_events": {
            "monitored_dir": "monitored_dir",
            "fix_subdirs": "fix_subdirs",
            "create_fix_subdirs": "create_fix_subdirs",
            "monitored_events": "monitored_events",
            "history_size": "history_size",
            "use_cleanup": "use_cleanup",
            "action_time": "action_time",
            "time_till_closed": "time_till_closed"
        },
        "watchdog_events": {
            "monitored_dir": "monitored_dir",
            "fix_subdirs": "fix_subdirs",
            "create_fix_subdirs": "create_fix_subdirs",
            "monitored_events": "monitored_events",
            "action_time": "action_time",
            "time_till_closed": "time_till_closed"
        },
        "http_events": {
            "fix_subdirs": "fix_subdirs",
            "history_size": "history_size",
            "det_ip": "det_ip",
            "det_api_version": "det_api_version"
        },
        "zmq_events": {
            "number_of_streams": "number_of_streams",
            "ext_data_port": "ext_data_port"
        },
        "hidra_events": {
            "ext_data_port": "ext_data_port"
        }
    },
    "datafetcher": {
        "type": "datafetcher_type",
        "datafetcher_port": "datafetcher_port",
        "status_check_port": "status_check_port",
        "status_check_resp_port": "status_check_resp_port",
        "confirmation_port": "confirmation_port",
        "confirmation_resp_port": "confirmation_resp_port",
        "chunksize": "chunksize",
        "router_port": "router_port",
        "cleaner_port": "cleaner_port",
        "cleaner_trigger_port": "cleaner_trigger_port",
        "use_data_stream": "use_data_stream",
        "data_stream_targets": "data_stream_targets",
        "number_of_streams": "number_of_streams",
        "remove_data": "remove_data",
        "store_data": "store_data",
        "local_target": "local_target",
        "file_fetcher": {
            "fix_subdirs": "fix_subdirs",
            "store_data": "store_data",
            "local_target": "local_target"
        },
        "http_fetcher": {
            "store_data": "store_data",
            "local_target": "local_target"
        }
    }
}


CONFIG_MAPPING_RECEIVER = {
    "general": {
        "log_size": "log_size",
        "log_path": "log_path",
        "log_name": "log_name",
        "username": "username",
        "procname": "procname",
        "ldapuri": "ldapuri",
        "ldap_retry_time": "ldap_retry_time",
        "netgroup_check_time": "netgroup_check_time",
        "dirs_not_to_create": "dirs_not_to_create",
        "whitelist": "whitelist",
        "verbose": "verbose",
        "onscreen": "onscreen"
    },
    "datareceiver": {
        "target_dir": "target_dir",
        "data_stream_ip": "data_stream_ip",
        "data_stream_port": "data_stream_port"
    }
}


def map_conf_format(flat_config, config_type, is_namespace=False):
    """
    A workaround to keep backwards compatibility to config file
    format.

    Args:
        flat_config: A flat dictionary containing the configuration parameters.
        config_type: What type of mapping should be used
        is_namespace: flat_config is not a dictionary but a namespace object.

    Returns:
        The configuration as dictionary in the correct hierarchy.
    """

    if config_type == "sender":
        mapping = CONFIG_MAPPING_SENDER
    elif config_type == "receiver":
        mapping = CONFIG_MAPPING_RECEIVER
    elif config_type is not None:
        mapping = config_type
    else:
        raise NotSupported("Config type is not supported")

    def _traverse_dict(config):
        to_delete = []
        for key, value in config.items():
            if isinstance(value, dict):
                if value:
                    _traverse_dict(value)
                    if not value:
                        to_delete.append(key)
            else:
                try:
                    # value of the mapping is the actual key of the flat
                    config[key] = flat_config[value]
                except KeyError:
                    to_delete.append(key)

        for key in to_delete:
            del config[key]

    # coming from command line arguments
    if is_namespace:
        arguments = copy.deepcopy(flat_config)
        flat_config = {}

        # arguments set when the program is called have a higher priority than
        # the ones in the config file
        for arg in vars(arguments):
            arg_value = getattr(arguments, arg)
            if arg_value is not None:
                if isinstance(arg_value, str):
                    if arg_value.lower() == "none":
                        flat_config[arg] = None
                    elif arg_value.lower() == "false":
                        flat_config[arg] = False
                    elif arg_value.lower() == "true":
                        flat_config[arg] = True
                    else:
                        flat_config[arg] = arg_value
                else:
                    flat_config[arg] = arg_value

            config = copy.deepcopy(mapping)
            _traverse_dict(config)

    else:
        is_flat = "general" not in flat_config
#        is_flat = all(not isinstance(value, dict)
#                      for key, value in flat_config.items()
#                      if key not in ["fix_subdirs", "monitored_events"])

        if is_flat:

            # fix backwards compatibility to version 4.0.16
            attr_to_change = [
                ("event_detector", "eventdetector"),
                ("event_detector_type", "eventdetector_type"),
                ("event_det_port", "eventdetector_port"),
                ("data_fetcher", "datafetcher"),
                ("data_fetcher_type", "datafetcher_type"),
                ("data_fetcher_port", "datafetcher_port")
            ]

            for old, new in attr_to_change:
                if old in flat_config:
                    flat_config[new] = copy.deepcopy(flat_config[old])
                    del flat_config[old]

            config = copy.deepcopy(mapping)
            _traverse_dict(config)
        else:
            config = flat_config

    return config


def set_flat_param(param, param_value, config, config_type, log=logging):
    """
    A workaround to keep backwards compatibility to config file
    format and control api (v4.0.x).

    Args:
        param: The parameter to set.
        param_value: The value to set the parameter to.
        config: A dictionary in hierarchical structure.
        config_type: What type of mapping should be used.
        log (optional): A log handler to use.
    """

    if config_type == "sender":
        mapping = CONFIG_MAPPING_SENDER
    elif config_type == "receiver":
        mapping = CONFIG_MAPPING_RECEIVER
    elif config_type is not None:
        mapping = config_type
    else:
        raise NotSupported("Config type is not supported")

    def _traverse_dict(config, mapping):
        for key, value in mapping.items():
            try:
                if isinstance(value, dict):
                    found = _traverse_dict(config[key], value)
                    if found:
                        return found
                else:
                    if value == param:
                        config[key] = param_value
                        found = True
                        return found
            except KeyError:
                pass

        return False

    found = _traverse_dict(config, mapping)
    if not found:
        log.debug("config=%s", config)
        raise Exception("Could not map flat parameter {}".format(param))


def get_flat_param(param, config, config_type, log=logging):
    """
    A workaround to keep backwards compatibility to config file
    format and control api (v4.0.x).

    Args:
        param: The parameter to get.
        config: A dictionary in hierarchical structure.
        config_type: What type of mapping should be used.
        log (optional): A log handler to use.

    Returns:
        The value corresponding to the key
    """

    if config_type == "sender":
        mapping = CONFIG_MAPPING_SENDER
    elif config_type == "receiver":
        mapping = CONFIG_MAPPING_RECEIVER
    elif config_type is not None:
        mapping = config_type
    else:
        raise NotSupported("Config type is not supported")

    def _traverse_dict(config, mapping):
        for key, value in mapping.items():
            try:
                if isinstance(value, dict):
                    found, param_value = _traverse_dict(config[key], value)
                    if found:
                        return found, param_value
                else:
                    if value == param:
                        return True, config[key]
            except KeyError:
                pass

        return False, None

    found, param_value = _traverse_dict(config, mapping)
    if found:
        return param_value
    else:
        log.debug("config=%s", config)
        raise Exception("Could not map flat parameter {}".format(param))


def _check_param(param_list, config, error_msg, log):
    check_passed = True
    config_reduced = {}

    for param in param_list:

        if isinstance(param, dict):
            check_passed, config_reduced = _check_params_dict(
                param, config, error_msg, log
            )
            continue

        # if the type of the parameter should be checked as well the entry
        # is a list instead of a string
        if isinstance(param, list):
            # if the type is checked the entry is of the form
            # [<param_name>, <param_type>]
            # or [<param_name>, <list of param values>]
            param_name, param_type = param
        else:
            param_name, param_type = param, None

        if param_name not in config:
            log.error("%s Missing parameter: '%s'",
                      error_msg, param_name)
            check_passed = False
            continue

        # check param type or value
        elif param_type is not None:

            # check if the parameter is one of the supported values
            if isinstance(param_type, list):
                if config[param_name] not in param_type:
                    log.error("%s Options for parameter '%s' are %s",
                              error_msg, param_name, param_type)
                    log.debug("parameter '%s' = %s", param_name,
                              config[param_name])
                    check_passed = False
                    continue

            # check if the parameter has the supported type
            elif not isinstance(config[param_name], param_type):
                log.error("%s Parameter '%s' is of format '%s' but should "
                          "be of format '%s'",
                          error_msg, param_name,
                          type(config[param_name]), param_type)
                check_passed = False
                continue

        config_reduced[param_name] = config[param_name]

    return check_passed, config_reduced


def _check_params_dict(required_params, config, msg, log):
    check_passed = True
    config_reduced = {}

    for key, value in required_params.items():
        error_msg = msg

        # check general format
        if key not in config:
            log.error("%s Missing section: '%s'", error_msg, key)
            check_passed = False
            continue

        error_msg = "{} (section '{}')".format(error_msg, key)

        if isinstance(value, dict):
            sub_check_passed, sub_config_reduced = _check_params_dict(
                value, config[key], error_msg, log
            )
        else:
            sub_check_passed, sub_config_reduced = _check_param(
                value, config[key], error_msg, log
            )

        check_passed = check_passed and sub_check_passed
        config_reduced[key] = sub_config_reduced

    return check_passed, config_reduced


def check_config(required_params, config, log, serialize=True):
    """Check the configuration.

    Args:

        required_params (list): list which can contain multiple formats
            - string: check if the parameter is contained
            - list of the format [<name>, <format>]: checks if the parameter
                is contained and has the right format
            - list of the format [<name>, <list of options>]: checks if the
                parameter is contained and set to supported values
        config (dict): dictionary where the configuration is stored
        log (class Logger): Logger instance of the module logging
        serialize (bool, optional): if the reduced config should be a
                                    serialized string or not

    Returns:

        check_passed: if all checks were successful
        config_reduced (str or dict): serialized (only if serialize=True) dict
                                      containing the values of the required
                                      parameters only
    """

    check_passed = True
    config_reduced = {}

    error_msg = "Configuration of wrong format."

    if not required_params:
        return check_passed, config_reduced

    if isinstance(required_params, list):
        check_passed, config_reduced = _check_param(
            required_params, config, error_msg, log
        )

    elif isinstance(required_params, dict):
        check_passed, config_reduced = _check_params_dict(
            required_params, config, error_msg, log
        )
    else:
        log.error("%s required_params has wrong input format.",
                  error_msg)
        check_passed = False

    if serialize:
        dict_to_str = build_serialized_config(config_reduced)
    else:
        dict_to_str = config_reduced

    return check_passed, dict_to_str


def build_serialized_config(config_reduced):
    """Serialize configuration into an easily readable string.

    Args:
        config_reduced (dict): The configuration dictionary to serialize.

    Returns:
        The serialized dictionary as string.
    """

    try:
        dict_to_str = str(
            json.dumps(config_reduced, sort_keys=True, indent=4)
        )
    except TypeError:
        # objects like e.g. zmq.context are not JSON serializable
        # convert manually
        sorted_keys = sorted(config_reduced.keys())
        indent = 4

        # putting it into a list first and the join it if more efficient
        # than string concatenation
        dict_to_list = []
        for key in sorted_keys:
            value = config_reduced[key]
            if isinstance(value, Endpoints):
                new_value = json.dumps(value._asdict(),
                                       sort_keys=True,
                                       indent=2 * 4)
                as_str = "{}{}: {}".format(" " * indent, key, new_value)
                # fix indentation
                as_str = as_str[:-1] + " " * indent + "}"
            else:
                as_str = "{}{}: {}".format(" " * indent, key, value)

            dict_to_list.append(as_str)

        # pylint: disable=redefined-variable-type
        dict_to_str = "{\n"
        dict_to_str += ",\n".join(dict_to_list)
        dict_to_str += "\n}"

    return dict_to_str


# ----------------------------------------------------------------------------
# configparser specific functions
# ----------------------------------------------------------------------------

# source:
# pylint: disable=line-too-long
# http://stackoverflow.com/questions/2819696/parsing-properties-file-in-python/2819788#2819788  # noqa E501
class _FakeSecHead(object):
    """Adds a fake section had to the configuration.

    This function is needed because configParser always needs a section name
    but the used config file consists of key-value pairs only
    """

    # pylint: disable=missing-docstring
    # pylint: disable=too-few-public-methods
    # pylint: disable=invalid-name

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


def set_parameters(base_config_file, config_file=None, arguments=None):
    """
    Merges configuration parameters from different sources.
    Hierarchy: base config overwritten by config overwritten by arguments.

    Args:
        base_config_file (str): Absolute path to the base configuration file.
        config_file (str): Absolute path to the configuration file to
                           overwrite the base configuration.
        arguments (dict): Arguments with highest priority.
    """

    base_config = load_config(base_config_file)

    if config_file is not None:
        config = load_config(config_file)

        # overwrite base config parameters with the ones in the config_file
        for key in config:
            base_config[key] = config[key]

    # arguments set when the program is called have a higher priority than
    # the ones in the config file
    if arguments is not None:
        for arg in vars(arguments):
            arg_value = getattr(arguments, arg)
            if arg_value is not None:
                if isinstance(arg_value, str):
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


def _parsing_error(section, option):
    # Raise an exception after a parsing error.
    raise RuntimeError(
        "Error parsing parameter {0} in section [{1}]. Make sure that the "
        "syntax is correct: list elements must be separated by commas and "
        "dict entries must contain the colon symbol. Strings must be quoted, "
        "even in lists and dicts.".format(option, section)
    )


def parse_parameters(config, log=logging):
    """Parses the parameters into a dictionary and fixes the types.

    Args:
        config (class RawConfigParser): ConfigParser instance.
        log (optional): A log handler to use.

    Returns:
        A dictionary containing the parameters.
    """

    config_dict = {}

    try:
        for sect in config.sections():
            config_dict[sect] = {}

            for opt in config.options(sect):
                config_dict[sect][opt] = config.get(sect, opt)
    except Exception:
        log.debug("config=%s", config)
        log.error("Could not parse parameters du to wrong format of "
                  "config file")
        raise

    return _convert_parameters(config_dict)


def _convert_parameters(config_dict):
    """Convert strings in parameter dictionaries to the correct data type.

    Convert a dictionary extracted by the configparse module to a
    dictionary contaning the same parameters converted from strings to
    correct types (int, float, string, etc.)

    Try to convert each entry in the dictionary according to the
    following rules. The first rule that applies to the entry
    determines the type.

    - If the entry starts and ends with a single quote or double quote,
      leave it as a string.
    - If the entry starts and ends with a square bracket, convert it to
      a list.
    - If the entry starts and ends with a curly braces, convert it to a
      dictionary or a set.
    - If the entry is the word None, without quotes, convert it to
      NoneType.
    - If the entry is the word False, without quotes, convert it to a
      boolean False.
    - If the entry is the word True, without quotes, convert it to a
      boolean True.
    - If none of the previous options match the content of the entry,
      try to interpret the entry in order as:

        - An integer number.
        - A float number.
        - A string.

    - If all else fails, raise an exception.

    Args:

        config_dict (Dict[str]): a dictionary containing the parameters from
            a configuration file as extracted by the
            :obj:`configparser` module).

    Returns:

        Dict: dictionary with the same structure as the input
        dictionary, but with correct types assigned to each entry.

    Raises:

        RuntimeError: if an entry cannot be converted to any supported
            type.
    """

    monitor_params = {}

    for section in config_dict.keys():
        monitor_params[section] = {}
        for option in config_dict[section].keys():
            recovered_option = config_dict[section][option]
            if (recovered_option.startswith("'")
                    and recovered_option.endswith("'")):
                monitor_params[section][option] = recovered_option[1:-1]
                continue

            if (recovered_option.startswith('"')
                    and recovered_option.endswith('"')):
                monitor_params[section][option] = recovered_option[1:-1]
                continue

            if (recovered_option.startswith("[")
                    and recovered_option.endswith("]")):
                try:
                    monitor_params[section][option] = ast.literal_eval(
                        # to fix raw literal string misinterpreting
                        recovered_option.replace("\\", "\\\\")
                    )
                    continue
                except (SyntaxError, ValueError):
                    _parsing_error(section, option)

            if (recovered_option.startswith("{")
                    and recovered_option.endswith("}")):
                try:
                    monitor_params[section][option] = ast.literal_eval(
                        # to fix raw literal string misinterpreting
                        recovered_option.replace("\\", "\\\\")
                    )
                    continue
                except (SyntaxError, ValueError):
                    _parsing_error(section, option)

            if recovered_option == 'None':
                monitor_params[section][option] = None
                continue

            if recovered_option == 'False':
                monitor_params[section][option] = False
                continue

            if recovered_option == 'True':
                monitor_params[section][option] = True
                continue

            try:
                monitor_params[section][option] = int(recovered_option)
                continue
            except ValueError:
                try:
                    monitor_params[section][option] = float(
                        recovered_option
                    )
                    continue
                except ValueError:
                    monitor_params[section][option] = recovered_option

    return monitor_params
