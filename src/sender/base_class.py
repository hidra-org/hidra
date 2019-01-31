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
This module implements the base class from which all classes of the sender
inherit from.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import _environment  # noqa F401 # pylint: disable=unused-import
import hidra.utils as utils
from hidra.utils import WrongConfiguration


# ------------------------------ #
#  Base class for ZMQ handling   #
# ------------------------------ #

class Base(object):
    """
    Implementation of the sender base class.
    """

    def __init__(self):
        self.log = None
        self.context = None

        self.required_params_base = {}
        self.required_params_dep = {}
        self.config_reduced = {}

    def _base_check(self, module_class, module_type, check_dep=True):
        """
        eg. module_class is "eventdetector" and module_type is "type"
        """

        # Check format of base config
        self.config_reduced = self._check_config_base(
            config=self.config_all,
            required_params=self.required_params_base
        )

        # Check format of dependent config
        if check_dep:
            self.required_params_dep = {
                module_class: [self.config_all[module_class][module_type]]
            }

            config_reduced_dep = self._check_config_base(
                config=self.config_all,
                required_params=[self.required_params_base,
                                 self.required_params_dep],
            )

            self.config_reduced.update(config_reduced_dep)
        else:
            self.required_params_dep = {}

    def _check_config_base(self, config, required_params):
        """
        Check a list of required parameters (in order) and if successful
        combines the results.

        Args:
            config (dict): The configuration dictionary to test.
            required_params (dict or list): The parameters that should be
                                            contained.

        Returns:
            A dictionary containing the values for the necessary parameters
            only.

        Raises:
            WrongConfiguration: The configuration has missing or
                                wrong parameters.
        """

        error_msg = "The configuration has missing or wrong parameters."

        config_reduced = {}

        if isinstance(required_params, dict):
            required_params = [required_params]
        elif not isinstance(required_params, list):
            raise WrongConfiguration("Wrong input, required_params has to "
                                     "be list or dict.")

        for params in required_params:
            check_passed, config_part = utils.check_config(
                required_params=params,
                config=config,
                log=self.log,
                serialize=False
            )

            if check_passed:
                config_reduced.update(config_part)
            else:
                raise WrongConfiguration(error_msg)

        return config_reduced




    def start_socket(self, name, sock_type, sock_con, endpoint, message=None):
        """Wrapper of start_socket

        Args:
            name: The name of the socket (used in log messages).
            sock_type: ZMQ socket type (e.g. zmq.PULL).
            sock_con: ZMQ binding type (connect or bind).
            endpoint: ZMQ endpoint to connect to.
            message (optional): wording to be used in the message
                                (default: Start).

        Returns:
            The ZMQ socket with the specified properties.
        """

        return utils.start_socket(
            name=name,
            sock_type=sock_type,
            sock_con=sock_con,
            endpoint=endpoint,
            context=self.context,
            log=self.log,
            message=message,
        )

    def stop_socket(self, name, socket=None):
        """Wrapper for stop_socket.

        Args:
            name: The name of the socket (used in log messages).
            socket (optional): The ZMQ socket to be closed.


        Returns:
            None if the socket was closed.
        """

        # use the class attribute
        if socket is None:
            try:
                socket = getattr(self, name)
                use_class_attribute = True
            except AttributeError:
                # socket does not exist thus cannot be removed
                return None
        else:
            use_class_attribute = False

        return_socket = utils.stop_socket(name=name,
                                          socket=socket,
                                          log=self.log)

        # class attributes are set directly
        if use_class_attribute:
            setattr(self, name, return_socket)
        else:
            return return_socket
