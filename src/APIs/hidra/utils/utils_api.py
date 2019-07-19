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
This module provides utilities used in the hidra APIs.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import copy
import json

from .utils_network import start_socket, stop_socket


class Base(object):
    """The base class from which all API classes should inherit from.
    """
    # pylint: disable=too-few-public-methods

    def __init__(self):
        self.log = None
        self.context = None

    def _start_socket(self,
                      name,
                      sock_type,
                      sock_con,
                      endpoint,
                      is_ipv6=False,
                      zap_domain=None,
                      random_port=None,
                      socket_options=None,
                      message=None):
        """Wrapper of start_socket.
        """

        if socket_options is None:
            socket_options = []

        socket, _ = start_socket(
            name=name,
            sock_type=sock_type,
            sock_con=sock_con,
            endpoint=endpoint,
            context=self.context,
            log=self.log,
            is_ipv6=is_ipv6,
            zap_domain=zap_domain,
            random_port=random_port,
            socket_options=socket_options,
            message=message
        )

        return socket

    def _stop_socket(self, name, socket=None):
        """Closes a zmq socket. Wrapper for stop_socket.

        Args:
            name: The name of the socket (used in log messages).
            socket: The ZMQ socket to be closed.
        """

        # use the class attribute
        if socket is None:
            socket = getattr(self, name)
            use_class_attribute = True
        else:
            use_class_attribute = False

        # close socket
        return_socket = stop_socket(name=name, socket=socket, log=self.log)

        # class attributes are set directly
        if use_class_attribute:
            setattr(self, name, return_socket)
        else:
            return return_socket

    def print_config(self, config, description=None):
        """
        Print the configuration in a user friendly way
        """

        formated_config = copy.deepcopy(config)
#        try:
#            # for better readability of named tuples
#            formated_config["network"]["endpoints"] = (
#                formated_config["network"]["endpoints"]._asdict()
#            )
#        except KeyError:
#            pass

        try:
            formated_config = str(json.dumps(formated_config,
                                             sort_keys=True,
                                             indent=4))
        except TypeError:
            # is thrown if one of the entries is not json serializable,
            # e.g happens for zmq context
            pass

        if description is None:
            self.log.info("Configuration for %s: %s",
                          self.__class__.__name__, formated_config)
        else:
            self.log.info("%s %s", description, formated_config)
