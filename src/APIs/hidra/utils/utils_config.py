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

from .utils_datatypes import Endpoints

try:
    import ConfigParser
except ImportError:
    # The ConfigParser module has been renamed to configparser in Python 3
    import configparser as ConfigParser


class WrongConfiguration(Exception):
    """Raised when something is wrong with the configuration.
    """
    pass


def load_config(config_file, config_type=None, log=logging):
    """Read and parse configuration data from the file.

    Args:
        config_file (str): Absolute path to the configuration file.
        config_type (str): The type the configuration is in (config or yaml).
                           If not set (or set to None) the file extenstion is
                           used for automatic detection.

    Returns:
        Configuration dictionary.

    """

    # Auto-detection
    if config_type is None:
        if config_file.endswith(".conf"):
            file_type = "conf"
        elif config_file.endswith(".yaml"):
            file_type = "yaml"
    else:
        file_type = config_type

    try:
        if file_type == "conf":
            configparser = ConfigParser.RawConfigParser()
            try:
                configparser.readfp(_FakeSecHead(open(config_file)))
            # TODO why was this necessary?
            except:  # pylint: disable=bare-except
                with open(config_file, 'r') as open_file:
                    config_string = '[asection]\n' + open_file.read()
                configparser.read_string(config_string)

            config = parse_parameters(configparser)["asection"]

        elif file_type == "yaml":
            with open(config_file) as f:
                config = yaml.load(f)

            # check for "None" entries
            _fix_none_entries(dictionary=config)
    except Exception:
        log.error("Could not load config file %s", config_file)
        raise

    return config


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
    """Updated one dictionary recursively with the entires of another.

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


def map_conf_format(flat_config, is_namespace=False):
    """
    A temporary workaround to keep backwards compatibility to config file
    format.

    Args:
        flat_config: A flat dictionary containing the configuration parameters.

    Returns:

    """

    mapping = {
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
            "eventdetector_type": "eventdetector_type",
            "ext_data_port": "ext_data_port",
            "event_det_port": "event_det_port",
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
            "datafetcher_type": "datafetcher_type",
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
                "store_data": "store_data",
                "local_target": "local_target"
            },
            "http_fetcher": {
                "store_data": "store_data",
                "local_target": "local_target"
            }
        }
    }

    def _traverse_dict(config):
        for key, value in config.items():
            if isinstance(value, dict):
                _traverse_dict(value)
            else:
                try:
                    config[key] = flat_config[key]
                except KeyError:
                    del config[key]

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
#        is_flat = all(not isinstance(value, dict)
#                      for key, value in flat_config.items()
#                      if key not in ["fix_subdirs", "monitored_events"])
        is_flat = "general" not in flat_config

        if is_flat:
            config = copy.deepcopy(mapping)
            _traverse_dict(config)
        else:
            config = flat_config

    return config


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

        check_passed: if all checks were successfull
        config_reduced (str or dict): serialized (only if serialize=True) dict
                                      containing the values of the required
                                      parameters only
    """

    check_passed = True
    config_reduced = {}

    def _check_param(param_list, config, error_msg):
        check_passed = True
        config_reduced = {}

        for param in param_list:

            if isinstance(param, dict):
                check_passed, config_reduced = _check_params_dict(
                    param, config, error_msg
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
            check_passed = True

        return check_passed, config_reduced

    def _check_params_dict(required_params, config, msg):
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
                    value, config[key], error_msg
                )
            else:
                sub_check_passed, sub_config_reduced = _check_param(
                    value, config[key], error_msg
                )

            check_passed = check_passed and sub_check_passed
            config_reduced[key] = sub_config_reduced

        return check_passed, config_reduced

    error_msg = "Configuration of wrong format."

    if isinstance(required_params, list):
        check_passed, config_reduced = _check_param(
            required_params, config, error_msg
        )

    elif isinstance(required_params, dict):
        check_passed, config_reduced = _check_params_dict(
            required_params, config, error_msg
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
#http://stackoverflow.com/questions/2819696/parsing-properties-file-in-python/2819788#2819788  # noqa E501
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


def parse_parameters(config):
    """Parses the parameters into a dictionary and fixes the types.

    Args:
        config (class RawConfigParser): ConfigParser instance.

    Returns:
        A dictionary containing the parameters.
    """

    config_dict = {}

    for sect in config.sections():
        config_dict[sect] = {}

        for opt in config.options(sect):
            config_dict[sect][opt] = config.get(sect, opt)

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

        config (Dict[str]): a dictionary containing the parameters from
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
