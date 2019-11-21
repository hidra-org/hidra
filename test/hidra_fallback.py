#!/usr/bin/env python

# This script serves as an example to achieve some kind of HA for HiDRA.
# It heavily relies on a Spectrum Scale cNFS cluster for IP failover.
# Failover will be triggered by Spectrum Scale, through a callback:
# mmaddcallback hidra_fallback --command /usr/local/sbin/hidra_fallback.py \
#  --event nodeJoin,nodeLeave --async -N cnfsNodes
# Only tested on CentOS 7

from __future__ import print_function

try:
    # The ConfigParser module has been renamed to configparser in Python 3
    from configparser import RawConfigParser
except ImportError:
    from ConfigParser import RawConfigParser

import logging
import os
import shlex
import socket
import StringIO
import subprocess
import sys
import time

CONFIG_PATH = "/opt/hidra/conf"
CONFIG_PREFIX = "receiver_"
CONFIG_POSTFIX = ".conf"
RECEIVER_PREFIX = "hidra-receiver@"
CONTROL_PREFIX = "hidra-control-server@"


def setup_logging():
    """Setup logging for stdout"""

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stdout,
    )


def call_initsystem(beamline, command, daemon):

    if not os.path.exists("/usr/lib/systemd"):
        print('Script not supported on non-systemd distributions')
        raise SystemExit(1)

    if command == "status":
        systemctl_command = ("systemctl is-active {0}{1}"
                             .format(daemon, beamline))
    else:
        systemctl_command = ("systemctl {0} {1}{2}"
                             .format(command, daemon, beamline))

    systemctl = subprocess.Popen(
        shlex.split(systemctl_command),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    systemctl.communicate()

    return_val = systemctl.returncode

    return return_val


def get_ip_addr():
    command = 'hostname -I'

    hostname = subprocess.Popen(
        shlex.split(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = hostname.communicate()

    # IPs are separated by a space and the output ends with a new line
    ip_list = stdout.split(" ")[:-1]

    active_ips = []
    ip_complete = []

    for ip in ip_list:

        # Version 1
        # p = subprocess.Popen(["nslookup", ip],
        #                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        #
        # # output of nslookup 131.169.185.121:
        # # Server:		131.169.40.200
        # # Address:	        131.169.40.200#53
        #
        # # 121.185.169.131.in-addr.arpa	name = zitpcx19282.desy.de.
        # # only the last line is needed without the new line
        # name_line = p.stdout.readlines()[-2]
        #
        # match_host = re.compile(r'[\w|.|-]+[\s]+name = ([\w|.|-]+).',
        #                         re.M | re.I)
        # netgroup = []
        #
        # if match_host.match(name_line):
        #     print ip, match_host.match(name_line).group(1)

        # Version 2
        try:
            ip_complete = socket.gethostbyaddr(ip)
        except Exception:
            pass

        for i in ip_complete:
            if type(i) == list:
                active_ips += i
            elif i not in active_ips:
                active_ips.append(i)

    return active_ips


def get_config(conf):
    config = RawConfigParser()

    with open(conf, 'r') as f:
        config_string = '[asection]\n' + f.read()

    try:
        config.read_string(config_string)
    except Exception:
        config_fp = StringIO.StringIO(config_string)
        config.read_file(config_fp)

    return config


def get_bls_to_check(active_ips):
    global CONFIG_PATH
    global CONFIG_PREFIX
    global CONFIG_POSTFIX

    files = [[os.path.join(CONFIG_PATH, f),
              f[len(CONFIG_PREFIX):-len(CONFIG_POSTFIX)]]
             for f in os.listdir(CONFIG_PATH)
             if f.startswith(CONFIG_PREFIX) and f.endswith(CONFIG_POSTFIX)]

    logging.info('Config files: %s', files)

    beamlines_to_activate = []
    beamlines_to_deactivate = []

    for conf, bl in files:

        config = get_config(conf)

        # the IP configured in the config file
        ip = remove_domain(config.get("asection", "data_stream_ip"))
        ip_found = False

        # get the beamline corresponding to the active ip
        for entry in active_ips:
            # remove domain for easier host comparison
            if ip == remove_domain(entry):
                ip_found = True
                # avoid multiple entries
                if bl not in beamlines_to_activate:
                    beamlines_to_activate.append(bl)

        if not ip_found:
            # avoid multiple entries
            if bl not in beamlines_to_deactivate:
                beamlines_to_deactivate.append(bl)

    return beamlines_to_activate, beamlines_to_deactivate


def remove_domain(fqdn):
    """Remove desy.de domain from FQDN"""

    return fqdn.replace(".desy.de", "")


def main():

    setup_logging()

    logging.info('HiDRA fail over initiated!')

    number_of_tries = 10
    i = 0
    active_ips = []
    while i < number_of_tries:
        # Get IP addresses
        active_ips = get_ip_addr()
        logging.info('Try: %s, discovered active IPs: %s', i, ', '
                     .join(active_ips))
        logging.debug('Sleeping for 5s')
        time.sleep(5)
        i += 1

    # mapping ip/hostname to beamline
    beamlines_to_activate, beamlines_to_deactivate = (
        get_bls_to_check(active_ips)
    )
    logging.info('List of beamline receiver to check (activate): %s',
                 ', '.join(beamlines_to_activate))

    logging.info('List of beamline receiver to check (deactivate): %s',
                 ', '.join(beamlines_to_deactivate))

    # check if hidra runs for this beamline
    # and start it if that is not the case
    for bl in beamlines_to_deactivate:
        for daemon in [RECEIVER_PREFIX, CONTROL_PREFIX]:
            p = call_initsystem(bl, "status", daemon)
            logging.debug('Return code for %s%s to deactivate: %s',
                          daemon, bl, p)

            if p != 0:
                logging.info('Service %s%s is not running', daemon, bl)
            else:
                logging.info('Service %s%s is running, will be stopped',
                             daemon, bl)
                # stop service
                call_initsystem(bl, "stop", daemon)

    # check if hidra runs for this beamline
    # and start it if that is not the case
    for bl in beamlines_to_activate:
        for daemon in [RECEIVER_PREFIX, CONTROL_PREFIX]:
            p = call_initsystem(bl, "status", daemon)
            logging.debug('Returncode for %s%s to activate: %s', daemon, bl, p)

            if p != 0:
                logging.info('Service %s%s is not running, will be started',
                             daemon, bl)
                # start service
                call_initsystem(bl, "start", daemon)
            else:
                logging.info('Service %s%s is running', daemon, bl)


if __name__ == '__main__':
    main()
