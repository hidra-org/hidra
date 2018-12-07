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
import json
import logging
import yaml

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
                           If not set the file extenstion is used for automatic
                           detection.

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
            config = ConfigParser.RawConfigParser()
            try:
                config.readfp(_FakeSecHead(open(config_file)))
            except:  # TODO why was this necessary? # pylint: disable=bare-except
                with open(config_file, 'r') as open_file:
                    config_string = '[asection]\n' + open_file.read()
                config.read_string(config_string)

        elif file_type == "yaml":
            with open(config_file) as f:
                config = yaml.load(f)

            # check for "None" entries
            _fix_none_entries(d=config)
    except Exception:
        log.error("Could not load config file %s", config_file)
        raise

    return config


def _fix_none_entries(d):
    """Converts all "None" entries in the dictionary to NoneType.

    Args:
        d (dict): The dictionary to travers.
    """

    for k, v in d.items():
        if isinstance(v, dict):
            _fix_none_entries(v)
        else:
            if v == "None":
                d[k] = None


def update_dict(d, d_to_update):
    """Updated one dictionary recursively with the entires of another.

    Args:
        d (dict): The dict used to update d_to_update.
        d_to_update (dict): The dictionary whose entries should be updated.
    """

    for k, v in d.items():
        if isinstance(v, dict):
            update_dict(v, d_to_update[k])
        else:
            d_to_update[k] = v


def check_config(required_params, config, log):
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

    Returns:

        check_passed: if all checks were successfull
        config_reduced (str): serialized dict containing the values of the
                              required parameters only
    """

    check_passed = True
    config_reduced = {}

    for param in required_params:
        # multiple checks have to be done
        if isinstance(param, list):
            # checks if the parameter is contained in the config dict
            if param[0] not in config:
                log.error("Configuration of wrong format. "
                          "Missing parameter '%s'", param[0])
                check_passed = False
            # check if the parameter is one of the supported values
            elif isinstance(param[1], list):
                if config[param[0]] not in param[1]:
                    log.error("Configuration of wrong format. Options for "
                              "parameter '%s' are %s", param[0], param[1])
                    log.debug("parameter '%s' = %s", param[0],
                              config[param[0]])
                    check_passed = False
            # check if the parameter has the supported type
            elif not isinstance(config[param[0]], param[1]):
                log.error("Configuration of wrong format. Parameter '%s' is "
                          "of format '%s' but should be of format '%s'",
                          param[0], type(config[param[0]]), param[1])
                check_passed = False
        # checks if the parameter is contained in the config dict
        elif param not in config:
            log.error("Configuration of wrong format. Missing parameter: '%s'",
                      param)
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

    return check_passed, dict_to_str


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
    Merges configration parameters from different sources.
    Hierarchy: base config overwritten by config overwritten by arguments.

    Args:
        base_config_file (str): Absolute path to the base configuration file.
        config_file (str): Absolute path to the configuration file to
                           overwrite the base configuration.
        arguments (dict): Arguments with highest priority.
    """

    base_config = parse_parameters(load_config(base_config_file))["asection"]

    if config_file is not None:
        config = parse_parameters(load_config(config_file))["asection"]

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
