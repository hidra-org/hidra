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
This module provides utilities use throughout different parts of hidra.
"""

from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import re
import socket as socket_m
import subprocess
import threading
import time

from .utils_datatypes import (
    IpcAddresses,  # noqa F401
    Endpoints,
    MAPPING_ZMQ_CONSTANTS_TO_STR,
    NotAllowed
)
from .utils_logging import LoggingFunction
from .utils_general import is_windows

_LOCK = threading.Lock()
_FQDN_CACHE = {}


def _get_fqdn(host, log):
    global _LOCK
    global _FQDN_CACHE

    try:
        # use cached one if possibly to reduce ldap queries
        fqdn = _FQDN_CACHE[host]
    except KeyError:
        with _LOCK:
            _FQDN_CACHE[host] = socket_m.getfqdn(host)
        fqdn = _FQDN_CACHE[host]
        log.debug("Cache fully qualified domain name (%s -> %s)",
                  host, fqdn)

    return fqdn


def check_netgroup(hostname,
                   beamline,
                   ldapuri,
                   netgroup_template,
                   log=None,
                   raise_if_failed=True):
    """Check if a host is in a netgroup belonging to a certain beamline.

    Args:
        hostname: The host to check.
        beamline: The beamline to which the host should belong to.
        ldapuri: Ldap node and port needed to check whitelist.
        netgroup_template: A template of the netgroup.
        log (optional): If the result should be logged.
            None: no logging
            False: use print
            anything else: use the standard logging module
        raise_if_failed (optional): Raise an exception the check fails.
    """

    if log is None:
        log = LoggingFunction(None)
    elif log:
        pass
    else:
        log = LoggingFunction("debug")

    netgroup_name = netgroup_template.format(bl=beamline)
    netgroup = execute_ldapsearch(log, netgroup_name, ldapuri)

    # convert host to fully qualified DNS name
    hostname = _get_fqdn(hostname, log)

    if hostname in netgroup:
        return True
    elif raise_if_failed:
        raise NotAllowed("Host {} is not contained in netgroup of beamline {}"
                         .format(hostname, beamline))
    else:
        return False


def _resolve_ldap_server_ip(log, ldapuri):
    # measure time to identify misconfigured network settings
    # (e.g. wrong resolve.conf)
    t_start = time.time()
    try:
        ldap_host = ldapuri.split(":")[0]
    except Exception:
        log.error("Failed to identify ldap host", exc_info=True)
        return None, None

    try:
        ldap_server_ip = socket_m.gethostbyname(ldap_host)
    except Exception:
        log.error("Failed to look up ldap ip", exc_info=True)
        return None, None

    ldap_response_limit = 9  # in sec
    if time.time() - t_start > ldap_response_limit:
        log.warning("Hostname resolution took quite long (longer that %s "
                    "sec). This can result in problems with connecting "
                    "services", ldap_response_limit)

    return ldap_server_ip, ldap_host


def execute_ldapsearch(log, ldap_cn, ldapuri):
    """Searches ldap for a netgroup and parses the output.

    Args:
        log: The log handler to use.
        ldap_cn: The ldap common name to search.
        ldapuri: Ldap node and port needed to check whitelist.

    Return:
        A list of hosts contained in the netgroup.
    """

    if ldap_cn is None or not ldap_cn:
        return []

    # if there were problems with ldapsearch these information are needed
    ldap_host, ldap_server_ip = _resolve_ldap_server_ip(log, ldapuri)

    proc = subprocess.Popen(
        ["ldapsearch",
         "-x",
         "-H ldap://" + ldapuri,
         "cn=" + ldap_cn, "-LLL"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    lines = proc.stdout.readlines()
    error = proc.stderr.read()

    if not lines and not error:
        log.debug("%s is not a netgroup, considering it as hostname", ldap_cn)
        return [_get_fqdn(ldap_cn, log)]

    netgroup = []
    try:
        match_host = re.compile(r'nisNetgroupTriple: [(]([\w|\S|.]+),.*,[)]',
                                re.M | re.I)
        for line in lines:
            line = line.decode()  # for python3 compatibility
            if match_host.match(line):
                if match_host.match(line).group(1) not in netgroup:
                    netgroup.append(
                        _get_fqdn(match_host.match(line).group(1), log)
                    )

        if error or not netgroup:
            log.error("Problem when using ldapsearch.")
            log.debug("stderr=%s", error)
            log.debug("stdout=%s", "".join(lines))
            log.debug("%s has the IP %s", ldap_host, ldap_server_ip)
    except Exception:
        # the code inside the try statement could not be tested properly so do
        # not stop if something was wrong.
        log.error("Not able to retrieve ldap error information.",
                  exc_info=True)

    return netgroup


def extend_whitelist(whitelist, ldapuri, log):
    """Only fully qualified domain named should be in the whitlist.

    Args:
        whitelist (list or str): List with host names, netgroup str
                                 or list of mixture of both.
        ldapuri (str): Ldap node and port needed to check whitelist.
        log: log handler

    Returns:
        The whitelist where the fully qualified domain name for all hosts
        contained is added.
    """

    log.info("Configured whitelist: %s", whitelist)

    if whitelist is None:
        return whitelist

    if isinstance(whitelist, str):
        whitelist = [whitelist]

    if is_windows():
        ext_whitelist = [_get_fqdn(host, log) for host in whitelist]
    else:
        ext_whitelist = []
        for i in whitelist:
            ext_whitelist += execute_ldapsearch(log, i, ldapuri)

    log.debug("Converted whitelist: %s", ext_whitelist)

    return ext_whitelist


def convert_socket_to_fqdn(socketids, log):
    """
    Converts hosts to fully qualified domain name

    Args:
        socketids (str): The socket ids to convert which where send by the
                         hidra API, e.g.
                         [["cfeld-pcx27533:50101", 1, ".*(tif|cbf)$"], ...]
                         or "my_host:50101"

        log: log handler

    Returns:
        socketids where the hostname was converted. E.g.
        [["my_host:50101", ...], ...] -> [["my_host.desy.de:50101", ...], ...]
        or "my_host:50101" -> "my_host.desy.de:50101"
    """

    if isinstance(socketids, list):
        for target in socketids:
            # socketids had the format
            # [["cfeld-pcx27533:50101", 1, ".*(tif|cbf)$"], ...]
            if isinstance(target, list):
                try:
                    host, port = target[0].split(":")
                except ValueError:
                    log.error("Target is of wrong format, either host or port "
                              "is missing")
                    raise

                new_target = "{}:{}".format(_get_fqdn(host, log), port)
                target[0] = new_target
    else:
        host, port = socketids.split(":")
        socketids = "{}:{}".format(_get_fqdn(host, log), port)

    log.debug("converted socketids=%s", socketids)

    return socketids


def is_ipv6_address(log, ip):  # pylint: disable=invalid-name
    """" Determines if given IP is an IPv4 or an IPv6 addresses

    Args:
        log: logger for the log messages
        ip: IP address to check

    Returns:
        boolean notifying if the IP was IPv4 or IPv6
    """
    # pylint: disable=invalid-name

    try:
        socket_m.inet_aton(ip)
        log.info("IPv4 address detected: %s.", ip)
        return False
    except socket_m.error:
        log.info("Address '%s' is not an IPv4 address, asume it is an IPv6 "
                 "address.", ip)
        return True


def get_socket_id(log, ip, port, is_ipv6=None):  # pylint: disable=invalid-name
    """ Determines socket ID for the given host and port

    If the IP is an IPV6 address the appropriate zeromq syntax is used.

    Args:
        log: logger for the log messages
        ip: The ip to use.
        port: The port to use.
        is_ipv6 (bool or None, optional): using the IPv6 syntax. If not set,
                                          the type of the IP is determined
                                          first.

    Returns:
        The socket id with the correct syntax.
    """

    if is_ipv6 is None:
        is_ipv6 = is_ipv6_address(log, ip)

    if is_ipv6:
        return "[{}]:{}".format(ip, port)
    else:
        return "{}:{}".format(ip, port)


def generate_sender_id(main_pid):
    """ Generates an unique id to identify the running datamanager.

    Args:
        main_pid: The PID of the datamanager

    Returns:
        A byte string containing the identifier
    """

    sender_id = "{}_{}".format(socket_m.getfqdn(), main_pid).encode("ascii")
    return sender_id


# ----------------------------------------------------------------------------
# Connection paths and strings
# ----------------------------------------------------------------------------

def set_ipc_addresses(ipc_dir, main_pid, use_cleaner=True):
    """Sets the ipc connection paths.

    Sets the connection strings  for the job, control, trigger and
    confirmation socket.

    Args:
        ipc_dir: Directory used for IPC connections
        main_pid: Process ID of the current process. Used to distinguish
                  different IPC connection.
        use_cleaner (optional): Boolean if the cleaner addresses should be set
    Returns:
        A namedtuple object IpcAddresses with the entries:
            control_pub,
            control_sub,
            request_fw,
            router,
            cleaner_job,
            cleaner_trigger,
            stats_in,
            stats_out
    """

    # determine socket connection strings
    ipc_ip = "{}/{}".format(ipc_dir, main_pid)

    control_pub = "{}_{}".format(ipc_ip, "control_pub")
    control_sub = "{}_{}".format(ipc_ip, "control_sub")
#    control_pub = "{}_{}".format(ipc_ip, "control_pub")
#    control_sub = "{}_{}".format(ipc_ip, "control_sub")
    request_fw = "{}_{}".format(ipc_ip, "request_fw")
#    request_fw = "{}_{}".format(ipc_ip, "request_fw")
    router = "{}_{}".format(ipc_ip, "router")
    stats_collect = "{}_{}".format(ipc_ip, "stats_collect")
    stats_expose = "{}_{}".format(ipc_ip, "stats_exposing")

    if use_cleaner:
        job = "{}_{}".format(ipc_ip, "cleaner")
        trigger = "{}_{}".format(ipc_ip, "cleaner_trigger")
    else:
        job = None
        trigger = None

    return IpcAddresses(
        control_pub=control_pub,
        control_sub=control_sub,
        request_fw=request_fw,
        router=router,
        cleaner_job=job,
        cleaner_trigger=trigger,
        stats_collect=stats_collect,
        stats_expose=stats_expose
    )


def set_endpoints(ext_ip,
                  con_ip,
                  ports,
                  ipc_addresses,
                  confirm_ips,
                  use_cleaner=True):
    """Configures the ZMQ address depending on the protocol.

    Sets the connection strings  for the job, control, trigger and
    confirmation socket.

    Args:
        ext_ip: IP to bind TCP connections to
        con_ip: IP to connect TCP connections to
        ports: A dictionary giving the ports to open TCP connection on
              (only used on Windows).
        ipc_addresses: Addresses to use for the IPC connections.
        confirm_ips: External ips/hostnames to bind and connect confirmation
                     socket to.
        use_cleaner (optional): Boolean which defines if the cleaner
            connections should be set (default: True)
    Returns:
        A namedtuple object Endpoints with the entries:
            control_pub_bind
            control_pub_con
            control_sub_bind
            control_sub_con
            request_bind,
            request_con,
            request_fw_bind,
            request_fw_con,
            router_bind,
            router_con,
            com_bind
            com_con
            cleaner_job_bind
            cleaner_job_con
            cleaner_trigger_bind
            cleaner_trigger_con
            confirm_bind
            confirm_con
            stats_collect_bind
            stats_collect_con
            stats_expose_bind
            stats_expose_con
    """

    # determine socket connection strings
    if is_windows():
        port = ports["control_pub"]
        control_pub_bind = "tcp://{}:{}".format(ext_ip, port)
        control_pub_con = "tcp://{}:{}".format(con_ip, port)

        port = ports["control_sub"]
        control_sub_bind = "tcp://{}:{}".format(ext_ip, port)
        control_sub_con = "tcp://{}:{}".format(con_ip, port)

        port = ports["request_fw"]
        request_fw_bind = "tcp://{}:{}".format(ext_ip, port)
        request_fw_con = "tcp://{}:{}".format(con_ip, port)

        port = ports["router"]
        router_bind = "tcp://{}:{}".format(ext_ip, port)
        router_con = "tcp://{}:{}".format(con_ip, port)

        stats_collect_bind = None
        stats_collect_con = None

        stats_expose_bind = None
        stats_expose_con = None

    else:
        control_pub_bind = "ipc://{}".format(ipc_addresses.control_pub)
        control_pub_con = control_pub_bind

        control_sub_bind = "ipc://{}".format(ipc_addresses.control_sub)
        control_sub_con = control_sub_bind

        request_fw_bind = "ipc://{}".format(ipc_addresses.request_fw)
        request_fw_con = request_fw_bind

        router_bind = "ipc://{}".format(ipc_addresses.router)
        router_con = router_bind

        stats_collect_bind = "ipc://{}".format(ipc_addresses.stats_collect)
        stats_collect_con = stats_collect_bind

        stats_expose_bind = "ipc://{}".format(ipc_addresses.stats_expose)
        stats_expose_con = stats_expose_bind

    if ports["request"] == "random":
        request_bind = "tcp://{}".format(ext_ip)
    else:
        request_bind = "tcp://{}:{}".format(ext_ip, ports["request"])
    request_con = "tcp://{}:{}".format(con_ip, ports["request"])

    if ports["com"] == "random":
        com_bind = "tcp://{}".format(ext_ip)
    else:
        com_bind = "tcp://{}:{}".format(ext_ip, ports["com"])
    com_con = "tcp://{}:{}".format(con_ip, ports["com"])

    # endpoints needed if cleaner is activated
    if use_cleaner:
        if is_windows():
            port = ports["cleaner"]
            job_bind = "tcp://{}:{}".format(ext_ip, port)
            job_con = "tcp://{}:{}".format(con_ip, port)

            port = ports["cleaner_trigger"]
            trigger_bind = "tcp://{}:{}".format(ext_ip, port)
            trigger_con = "tcp://{}:{}".format(con_ip, port)

        else:
            job_bind = "ipc://{}".format(ipc_addresses.cleaner_job)
            job_con = job_bind

            trigger_bind = "ipc://{}".format(ipc_addresses.cleaner_trigger)
            trigger_con = trigger_bind

        port = ports["confirmation"]
        confirm_bind = "tcp://{}:{}".format(confirm_ips[0], port)
        confirm_con = "tcp://{}:{}".format(confirm_ips[1], port)
    else:
        job_bind = None
        job_con = None
        trigger_bind = None
        trigger_con = None
        confirm_bind = None
        confirm_con = None

    return Endpoints(
        control_pub_bind=control_pub_bind,
        control_pub_con=control_pub_con,
        control_sub_bind=control_sub_bind,
        control_sub_con=control_sub_con,
        request_bind=request_bind,
        request_con=request_con,
        request_fw_bind=request_fw_bind,
        request_fw_con=request_fw_con,
        router_bind=router_bind,
        router_con=router_con,
        com_bind=com_bind,
        com_con=com_con,
        cleaner_job_bind=job_bind,
        cleaner_job_con=job_con,
        cleaner_trigger_bind=trigger_bind,
        cleaner_trigger_con=trigger_con,
        confirm_bind=confirm_bind,
        confirm_con=confirm_con,
        stats_collect_bind=stats_collect_bind,
        stats_collect_con=stats_collect_con,
        stats_expose_bind=stats_expose_bind,
        stats_expose_con=stats_expose_con,
    )


# ----------------------------------------------------------------------------
# zmq functions
# ----------------------------------------------------------------------------

def start_socket(name,
                 sock_type,
                 sock_con,
                 endpoint,
                 context,
                 log,
                 is_ipv6=False,
                 zap_domain=None,
                 random_port=None,
                 socket_options=None,
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
        random_port (optional): Use zmq bind_to_random_port option.
        socket_options (optional): Additional zmq options which should be set
            on the socket (as lists which [key, value] entries).
        message (optional): wording to be used in the message (default: Start).
    """

    if socket_options is None:
        socket_options = []

    if message is None:
        message = "Start"

    sock_type_as_str = MAPPING_ZMQ_CONSTANTS_TO_STR[sock_type]
    endpoint_to_print = endpoint
    port = None

    try:
        # create socket
        socket = context.socket(sock_type)

        # register the authentication domain
        if zap_domain:
            socket.zap_domain = zap_domain

        # enable IPv6 on socket
        if is_ipv6:
            socket.ipv6 = True
            log.debug("Enabling IPv6 socket for %s", name)

        # set socket options
        socket.linger = 0  # always
        for param, value in socket_options:
            socket.setsockopt(param, value)

        # connect/bind the socket
        if sock_con == "connect":
            socket.connect(endpoint)

        elif sock_con == "bind":
            if random_port is None or random_port is False:
                socket.bind(endpoint)
            else:
                log.debug("Enabling random port usage for %s", name)
                port = socket.bind_to_random_port(endpoint)
                endpoint_to_print = "{}:{}".format(endpoint, port)

        log.info("%s %s (%s, %s): '%s'", message, name, sock_con,
                 sock_type_as_str, endpoint_to_print)
    except Exception:
        log.error("Failed to %s %s (%s, %s): '%s'", name, message.lower(),
                  sock_con, sock_type_as_str, endpoint_to_print, exc_info=True)
        raise

    return socket, port


def stop_socket(name, socket, log):
    """Closes a zmq socket.

    Args:
        name: The name of the socket (used in log messages).
        socket: The ZMQ socket to be closed.
        log: Logger used for log messages.
    """

    # close socket
    if socket is not None:
        log.info("Closing %s", name)
        socket.close(linger=0)
        socket = None

    return socket
