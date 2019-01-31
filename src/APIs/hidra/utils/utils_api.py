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

from .utils_network import start_socket, stop_socket
from .utils_datatypes import (
    NotSupported,
    UsageError,
    FormatError,
    ConnectionFailed,
    VersionError,
    AuthenticationFailed,
    CommunicationFailed,
    DataSavingError
)


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
                      message=None):
        """Wrapper of start_socket.
        """

        return start_socket(
            name=name,
            sock_type=sock_type,
            sock_con=sock_con,
            endpoint=endpoint,
            context=self.context,
            log=self.log,
            is_ipv6=is_ipv6,
            zap_domain=zap_domain,
            message=message
        )

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
