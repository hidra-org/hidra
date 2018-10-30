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

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import __init__ as init  # noqa F401 # pylint: disable=unused-import
import utils


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
            socket = getattr(self, name)
            use_class_attribute = True
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
