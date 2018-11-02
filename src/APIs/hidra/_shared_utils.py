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
This module proovides utilities used in the hidra APIs.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import re
import socket as socket_m
import subprocess
import traceback


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
        """Closes a zmq socket.

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
        socket = stop_socket(name=name, socket=socket, log=self.log)

        # class attributes are set directly
        if use_class_attribute:
            setattr(self, name, socket)
        else:
            return socket


def execute_ldapsearch(log, ldap_cn, ldapuri):
    """Searches ldap for a netgroup and parses the output.

    Args:
        ldap_cn: The ldap common name to search.
        ldapuri: Ldap node and port needed to check whitelist.

    Return:
        A list of hosts contained in the netgroup.
    """

    # if there were problems with ldapsearch these information are needed
    try:
        ldap_host = ldapuri.split(":")[0]
        ldap_server_ip = socket_m.gethostbyname(ldap_host)
    except Exception:
        log.error("Failed to look up ldap ip", exc_info=True)

    proc = subprocess.Popen(
        ["ldapsearch",
         "-x",
         "-H ldap://" + ldapuri,
         "cn=" + ldap_cn, "-LLL"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    lines = proc.stdout.readlines()
    error = proc.stderr.read()

    match_host = re.compile(r'nisNetgroupTriple: [(]([\w|\S|.]+),.*,[)]',
                            re.M | re.I)
    netgroup = []

    for line in lines:
        if match_host.match(line):
            if match_host.match(line).group(1) not in netgroup:
                netgroup.append(match_host.match(line).group(1))

    try:
        if error or not netgroup:
            log.error("Problem when using ldapsearch.")
            log.debug("stderr={}".format(error))
            log.debug("stdout={}".format("".join(lines)))
            log.debug("{} has the IP {}".format(ldap_host, ldap_server_ip))
    except Exception:
        # the code inside the try statement could not be tested properly so do
        # not stop if something was wrong.
        log.error("Not able to retrieve ldap error information.",
                  exc_info=True)

    return netgroup


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


def start_socket(name,
                 sock_type,
                 sock_con,
                 endpoint,
                 context,
                 log,
                 is_ipv6=False,
                 zap_domain=None,
                 message=None):
    """Creates a zmq socket.

    Args:
        name: The name of the socket (used in log messages).
        sock_type: ZMQ socket type (e.g. zmq.PULL).
        sock_con: ZMQ binding type (connect or bind).
        endpoint: ZMQ endpoint to connect to.
        context: ZMQ context to create the socket on.
        log: Logger used for log messages.
        is_ipv6: Enable IPv6 on socket.
        zap_domain: The RFC 27 authentication domain used for ZMQ
                    communication.
        message (optional): wording to be used in the message
                            (default: Start).
    """

    if message is None:
        message = "Start"

    sock_type_as_str = MAPPING_ZMQ_CONSTANTS_TO_STR[sock_type]

    try:
        # create socket
        socket = context.socket(sock_type)

        # register the authentication domain
        if zap_domain:
            socket.zap_domain = zap_domain

        # enable IPv6 on socket
        if is_ipv6:
            socket.ipv6 = True
            log.debug("Enabling IPv6 socket for {}".format(name))

        # connect/bind the socket
        if sock_con == "connect":
            socket.connect(endpoint)
        elif sock_con == "bind":
            socket.bind(endpoint)

        log.info("{} {} ({}, {}): '{}'".format(message,
                                               name,
                                               sock_con,
                                               sock_type_as_str,
                                               endpoint))
    except:
        log.error("Failed to {} {} ({}, {}): '{}'".format(name,
                                                          message.lower(),
                                                          sock_con,
                                                          sock_type_as_str,
                                                          endpoint),
                  exc_info=True)
        raise

    return socket


def stop_socket(name, socket, log):
    """Closes a zmq socket.

    Args:
        name: The name of the socket (used in log messages).
        socket: The ZMQ socket to be closed.
        log: Logger used for log messages.
    """

    # close socket
    if socket is not None:
        log.info("Closing {}".format(name))
        socket.close(linger=0)
        socket = None

    return socket


# ------------------------------ #
#            Logging             #
# ------------------------------ #

class LoggingFunction(object):
    """Overwrites logging with print or suppresses it.
    """

    def __init__(self, level="debug"):
        if level == "debug":
            # using output
            self.debug = self.out
            self.info = self.out
            self.warning = self.out
            self.error = self.out
            self.critical = self.out
        elif level == "info":
            # using no output
            self.debug = self.no_out
            # using output
            self.info = self.out
            self.warning = self.out
            self.error = self.out
            self.critical = self.out
        elif level == "warning":
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            # using output
            self.warning = self.out
            self.error = self.out
            self.critical = self.out
        elif level == "error":
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            self.warning = self.no_out
            # using output
            self.error = self.out
            self.critical = self.out
        elif level == "critical":
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            self.warning = self.no_out
            self.error = self.no_out
            # using output
            self.critical = self.out
        elif level is None:
            # using no output
            self.debug = self.no_out
            self.info = self.no_out
            self.warning = self.no_out
            self.error = self.no_out
            self.critical = self.no_out

    def out(self, msg, exc_info=None):
        """Prints to screen.

        Args:
            msg: The message to print.
            exc_info: If a traceback should be printed in addition.
        """
        # pylint: disable=no-self-use

        if exc_info:
            print(msg, traceback.format_exc())
        else:
            print(msg)

    def no_out(self, msg, exc_info=None):
        """Print nothing.
        """
        pass
