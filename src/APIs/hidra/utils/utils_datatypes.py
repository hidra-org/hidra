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

from collections import namedtuple


# ------------------------------ #
#  Connection paths and strings  #
# ------------------------------ #

# To be pickable these have to be defined at the top level of a module
# this is needed because multiprocessing on windows needs these pickable.
# Additionally the name of the namedtuple has to be the same as the typename
# otherwise it cannot be pickled on Windows.


IpcAddresses = namedtuple(
    "IpcAddresses", [
        "control_pub",
        "control_sub",
        "request_fw",
        "router",
        "cleaner_job",
        "cleaner_trigger",
    ]
)


Endpoints = namedtuple(
    "Endpoints", [
        "control_pub_bind",
        "control_pub_con",
        "control_sub_bind",
        "control_sub_con",
        "request_bind",
        "request_con",
        "request_fw_bind",
        "request_fw_con",
        "router_bind",
        "router_con",
        "com_bind",
        "com_con",
        "cleaner_job_bind",
        "cleaner_job_con",
        "cleaner_trigger_bind",
        "cleaner_trigger_con",
        "confirm_bind",
        "confirm_con",
    ]
)


# ------------------------------ #
#         ZMQ functions          #
# ------------------------------ #

MAPPING_ZMQ_CONSTANTS_TO_STR = [
    "PAIR",  # zmq.PAIR = 0
    "PUB",  # zmq.PUB = 1
    "SUB",  # zmq.SUB = 2
    "REQ",  # zmq.REQ = 3
    "REP",  # zmq.REP = 4
    "DEALER/XREQ",  # zmq.DEALER/zmq.XREQ = 5
    "ROUTER/XREP",  # zmq.ROUTER/zmq.XREP = 6
    "PULL",  # zmq.PULL = 7
    "PUSH",  # zmq.PUSH = 8
    "XPUB",  # zmq.XPUB = 9
    "XSUB",  # zmq.XSUB = 10
]


# ------------------------------ #
#           Exceptions           #
# ------------------------------ #

class NotSupported(Exception):
    """Raised when a parameter is not supported."""
    pass


class UsageError(Exception):
    """Raised when API was used in a wrong way."""
    pass


class FormatError(Exception):
    """Raised when a parameter is of the wrong format."""
    pass


class ConnectionFailed(Exception):
    """Raised when the connection to hidra could not be established."""
    pass


class VersionError(Exception):
    """Raised when the api and the hidra version do not match."""
    pass


class AuthenticationFailed(Exception):
    """Raised when the connection to hidra is not allowed."""
    pass


class CommunicationFailed(Exception):
    """
    Raised when a the connection to hidra is established but something was
    wrong with the communication.
    """
    pass


class DataSavingError(Exception):
    """Raised when an error occured while the data was saved."""
    pass


class WrongConfiguration(Exception):
    """Raised when something is wrong with the configuration.
    """
    pass
