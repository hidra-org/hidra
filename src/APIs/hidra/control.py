# API to communicate with a data transfer unit

from __future__ import print_function
# from __future__ import unicode_literals
from __future__ import absolute_import

import socket
import logging
import os
import sys
import subprocess
import re
import json
import zmq
from string import Template

# from ._version import __version__
from ._constants import connection_list
from ._shared_helpers import LoggingFunction

LDAPURI = "it-ldap-slave.desy.de:1389"
NETGROUP_TEMPLATE = Template("a3${bl}-hosts")


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

    if log is None:
        log = LoggingFunction(None)
    elif log:
        pass
    else:
        log = LoggingFunction("debug")

    netgroup_name = NETGROUP_TEMPLATE.substitute(bl=beamline)
    netgroup = excecute_ldapsearch(netgroup_name)

    # convert host to fully qualified DNS name
    hostname = socket.getfqdn(hostname)

    if hostname not in netgroup:
        log.error("Host {0} is not contained in netgroup of "
                  "beamline {1}".format(hostname, beamline))
        sys.exit(1)


class Control():
    def __init__(self, beamline, detector, use_log=False):

        # print messages of certain level to screen
        if use_log in ["debug", "info", "warning", "error", "critical"]:
            self.log = LoggingFunction(use_log)
        # use logging
        elif use_log:
            self.log = logging.getLogger("Control")
        # use no logging at all
        elif use_log is None:
            self.log = LoggingFunction(None)
        # print everything to screen
        else:
            self.log = LoggingFunction("debug")

        self.current_pid = os.getpid()

        self.beamline = beamline
        self.detector = detector
        self.socket = None

        self.host = socket.getfqdn()

        check_netgroup(self.host, self.beamline, self.log)
        check_netgroup(self.detector, self.beamline, self.log)

        try:
            self.con_id = "tcp://{0}:{1}".format(
                connection_list[self.beamline]["host"],
                connection_list[self.beamline]["port"])
            self.log.info("Starting connection to {0}".format(self.con_id))
        except:
            self.log.error("Beamline {0} not supported".format(self.beamline))
            sys.exit(1)

        self.__create_sockets()

        self.__check_responding()

    def __create_sockets(self):

        # Create ZeroMQ context
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

    def __check_responding(self):
        test_signal = b"IS_ALIVE"
        tracker = self.socket.send_multipart([test_signal], zmq.NOBLOCK,
                                             copy=False, track=True)

        # test if someone picks up the test message in the next 2 sec
        if not tracker.done:
            try:
                tracker.wait(1)
            except zmq.error.NotDone:
                pass

        # no one picked up the test message
        if not tracker.done:
            self.log.error("HiDRA control server is not answering.")
            self.stop(unregister=False)
            sys.exit(1)

        responce = self.socket.recv()
        if responce == b"OK":
            self.log.info("HiDRA control server up and answering.")
        else:
            self.log.error("HiDRA control server is in failed state.")
            self.log.debug("responce was: {0}".format(responce))
            self.stop(unregister=False)
            sys.exit(1)

    def get(self, attribute, timeout=None):
        msg = [b"get", self.host, self.detector, attribute]

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

        if attribute == "det_ip":
            check_netgroup(value[0], self.beamline, self.log)

        if attribute == "whitelist":
            msg = [b"set", self.host, self.detector, attribute,
                   json.dumps(value)]
        else:
            msg = [b"set", self.host, self.detector, attribute,
                   json.dumps(value[0])]

        self.socket.send_multipart(msg)
        self.log.debug("sent: {0}".format(msg))

        reply = self.socket.recv()
        self.log.debug("recv: {0}".format(reply))

        return reply

    def do(self, command, timeout=None):
        msg = [b"do", self.host, self.detector, command]

        self.socket.send_multipart(msg)
        self.log.debug("sent: {0}".format(msg))

        reply = self.socket.recv()
        self.log.debug("recv: {0}".format(reply))

        return reply

    def stop(self, unregister=True):
        if self.socket is not None:
            if unregister:
                self.log.info("Sending close signal")
                msg = [b"bye", self.host, self.detector]

                self.socket.send_multipart(msg)
                self.log.debug("sent: {0}".format(msg))

                reply = self.socket.recv()
                self.log.debug("recv: {0} ".format(reply))

            try:
                self.log.info("closing socket...")
                self.socket.close(0)
                self.socket = None
            except:
                self.log.error("closing sockets...failed.", exc_info=True)

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


def reset_receiver_status(host, port):
    context = zmq.Context()

    try:
        reset_socket = context.socket(zmq.REQ)
        con_str = "tcp://{0}:{1}".format(host, port)

        reset_socket.connect(con_str)
        print("Start reset_socket (connect): '{0}'".format(con_str))
    except:
        print("Failed to start reset_socket (connect): '{0}'".format(con_str),
              exc_info=True)

    reset_socket.send_multipart([b"RESET_STATUS"])
    print("Reset request sent")

    responce = reset_socket.recv_multipart()
    print("Responce: {0}".format(responce))

    reset_socket.close()
    context.destroy()
