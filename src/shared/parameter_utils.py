#!/usr/bin/env python

# Copyright (C) 2015  DESY, Notkestr. 85, D-22607 Hamburg
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
# Parts of this file where former members of cfelpyutils.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#     Valerio Mariani <valerio.mariani@desy.de>

"""
Parameter parsing utilities.

This module contains the implementation of functions that are used to
parse and manipulate options and parameters, as extracted by the
python :obj:`configparse` module.
"""

from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import ast


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

    return convert_parameters(config_dict)


def convert_parameters(config_dict):
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
