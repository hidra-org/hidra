# API to communicate with a data transfer unit

from __future__ import print_function
# from __future__ import unicode_literals
from __future__ import absolute_import

import socket
import logging
import os
import sys
import traceback
import subprocess
import re
import json

# from ._version import __version__
from ._constants import connection_list


class LoggingFunction:
    def out(self, x, exc_info=None):
        if exc_info:
            print (x, traceback.format_exc())
        else:
            print (x)

    def __init__(self):
        self.debug = lambda x, exc_info=None: self.out(x, exc_info)
        self.info = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning = lambda x, exc_info=None: self.out(x, exc_info)
        self.error = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


class NoLoggingFunction:
    def out(self, x, exc_info=None):
        pass

    def __init__(self):
        self.debug = lambda x, exc_info=None: self.out(x, exc_info)
        self.info = lambda x, exc_info=None: self.out(x, exc_info)
        self.warning = lambda x, exc_info=None: self.out(x, exc_info)
        self.error = lambda x, exc_info=None: self.out(x, exc_info)
        self.critical = lambda x, exc_info=None: self.out(x, exc_info)


class NotSupported(Exception):
    pass


class UsageError(Exception):
    pass


class FormatError(Exception):
    pass


class ConnectionFailed(Exception):
    pass


class VersionError(Exception):
    pass


class AuthenticationFailed(Exception):
    pass


class CommunicationFailed(Exception):
    pass


def excecute_ldapsearch(ldap_cn):

    p = subprocess.Popen(
        ["ldapsearch",
         "-x",
         "-H ldap://it-ldap-slave.desy.de:1389",
         "cn=" + ldap_cn, "-LLL"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    lines = p.stdout.readlines()

    match_host = re.compile(r'nisNetgroupTriple: [(]([\w|\S|.]+),.*,[)]',
                            re.M | re.I)
    netgroup = []

    for line in lines:
        if match_host.match(line):
            if match_host.match(line).group(1) not in netgroup:
                netgroup.append(match_host.match(line).group(1))

    return netgroup


def check_netgroup(hostname, beamline, log=None):
    if log is None:
        log = NoLoggingFunction()
    else:
        log = LoggingFunction()

    netgroup_name = "a3{0}-hosts".format(beamline)

    netgroup = excecute_ldapsearch(netgroup_name)

    # not all hosts are configuered with a fully qualified DNS name
    hostname = hostname.replace(".desy.de", "")
    netgroup_modified = []
    for host in netgroup:
        netgroup_modified.append(host.replace(".desy.de", ""))

    if hostname not in netgroup_modified:
        log.error("Host {0} is not contained in netgroup of "
                  "beamline {1}".format(hostname, beamline))
        sys.exit(1)


class Control():
    def __init__(self, beamline, use_log=False):

        if use_log:
            self.log = logging.getLogger("Control")
        elif use_log is None:
            self.log = NoLoggingFunction()
        else:
            self.log = LoggingFunction()

        self.current_pid = os.getpid()

        self.beamline = beamline
        self.signal_socket = None

        check_netgroup(socket.gethostname(), self.beamline, self.log)

        try:
            self.signal_host = connection_list[beamline]["host"]
            self.signal_port = connection_list[beamline]["port"]
            self.log.info("Starting connection to {0} on port {1}"
                          .format(self.signal_host, self.signal_port))
        except:
            self.log.error("Beamline {0} not supported".format(self.beamline))

        self.__create_sockets()

    def __create_sockets(self):
        self.signal_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.signal_socket.connect((self.signal_host, self.signal_port))
        except Exception:
            self.log.error("connect() failed", exc_info=True)
            self.signal_socket.close()
            sys.exit(1)

    def get(self, attribute, timeout=None):
        msg = 'get {0}'.format(attribute)

        self.signal_socket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signal_socket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len(reply), reply))

        return json.loads(reply)

    def set(self, attribute, *value):
        value = list(value)

        # flatten list if entry was a list (result: list of lists)
        if type(value[0]) == list:
            value = [item for sublist in value for item in sublist]

        if attribute == "det_ip":
            check_netgroup(value[0], self.beamline, self.log)

        if attribute == "whitelist":
            msg = 'set {0} {1}'.format(attribute, value)
        else:
            msg = 'set {0} {1}'.format(attribute, value[0])

        self.signal_socket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signal_socket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len(reply), reply))

        return reply

    def do(self, command, timeout=None):
        msg = 'do {0}'.format(command)

        self.signal_socket.send(msg)
        self.log.debug("sent (len %2d): %s" % (len(msg), msg))

        reply = self.signal_socket.recv(1024)
        self.log.debug("recv (len %2d): %s " % (len(reply), reply))

        return reply

    def stop(self):
        if self.signal_socket:
            self.log.info("Sending close signal")
            msg = 'bye'

            self.signal_socket.send(msg)
            self.log.debug("sent (len %2d): %s" % (len(msg), msg))

            reply = self.signal_socket.recv(1024)
            self.log.debug("recv (len %2d): %s " % (len(reply), reply))

            try:
                self.log.info("closing signal_socket...")
                self.signal_socket.close()
                self.signal_socket = None
            except:
                self.log.error("closing sockets...failed.", exc_info=True)

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
