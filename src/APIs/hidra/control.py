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
import zmq

# from ._version import __version__
from ._constants import connection_list

DOMAIN = ".desy.de"
LDAPURI = "it-ldap-slave.desy.de:1389"

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
    global LDAPURI

    p = subprocess.Popen(
        ["ldapsearch",
         "-x",
         "-H ldap://" + LDAPURI,
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
    global DOMAIN

    if log is None:
        log = NoLoggingFunction()
    else:
        log = LoggingFunction()

    netgroup_name = "a3{0}-hosts".format(beamline)

    netgroup = excecute_ldapsearch(netgroup_name)

    # not all hosts are configuered with a fully qualified DNS name
    hostname = hostname.replace(DOMAIN, "")
    netgroup_modified = []
    for host in netgroup:
        netgroup_modified.append(host.replace(DOMAIN, ""))

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

        self.host = socket.gethostname()

        check_netgroup(self.host, self.beamline, self.log)

        try:
            self.con_id = "tcp://{0}:{1}".format(
                connection_list[self.beamline]["host"],
                connection_list[self.beamline]["port"])
            self.log.info("Starting connection to {0}".format(self.con_id))
        except:
            self.log.error("Beamline {0} not supported".format(self.beamline))
            sys.exit(1)

        self.__create_sockets()

    def __create_sockets(self):

        #Create ZeroMQ context
        self.log.info("Registering ZMQ context")
        self.context = zmq.Context()

        # socket to get requests
        try:
            self.socket = self.context.socket(zmq.REQ)
            self.socket.connect(self.con_id)
            self.log.info("Start socket (connect): '{0}'"
                          .format(self.con_id))
        except:
            self.log.error("Failed to start socket (connect): '{0}'"
                           .format(self.con_id), exc_info=True)
            raise

    def get(self, attribute, timeout=None):
        msg = [b"get", self.host, attribute]

        self.socket.send_multipart(msg)
        self.log.debug("sent: {0}".format(msg))

        reply = self.socket.recv()
        self.log.debug("recv: {0}".format(reply))

        return json.loads(reply)

    def set(self, attribute, *value):
        value = list(value)

        # flatten list if entry was a list (result: list of lists)
        if type(value[0]) == list:
            value = [item for sublist in value for item in sublist]

        if attribute == "eiger_ip":
            check_netgroup(value[0], self.beamline, self.log)

        if attribute == "whitelist":
            msg = [b"set", self.host, attribute, json.dumps(value)]
        else:
            msg = [b"set", self.host, attribute, json.dumps(value[0])]

        self.socket.send_multipart(msg)
        self.log.debug("sent: {0}".format(msg))

        reply = self.socket.recv()
        self.log.debug("recv: {0}".format(reply))

        return reply

    def do(self, command, timeout=None):
        msg = [b"do", self.host, command]

        self.socket.send_multipart(msg)
        self.log.debug("sent: {0}".format(msg))

        reply = self.socket.recv()
        self.log.debug("recv: {0}".format(reply))

        return reply

    def stop(self):
        if self.socket:
            self.log.info("Sending close signal")
            msg = [b"bye", self.host]

            self.socket.send_multipart(msg)
            self.log.debug("sent: {0}".format(msg))

            reply = self.socket.recv()
            self.log.debug("recv: {0} ".format(reply))

            try:
                self.log.info("closing socket...")
                self.socket.close()
                self.socket = None
            except:
                self.log.error("closing sockets...failed.", exc_info=True)

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
