from __future__ import print_function

import subprocess
#import re
import socket
import os
import StringIO
#import platform
try:
    import ConfigParser
except ImportError:
    # The ConfigParser module has been renamed to configparser in Python 3
    import configparser as ConfigParser


CONFIG_PATH = "/opt/hidra/conf"
CONFIG_PREFIX = "receiver_"
CONFIG_POSTFIX = ".conf"
SYSTEMD_PREFIX = "hidra-receiver@"
SERVICE_NAME = "hidra-receiver"


def call_initsystem(beamline, command):
    global SYSTEMD_PREFIX
    global SERVICE_NAME

#    dist = platform.dist()
    return_val = None

    # source for the differentiation: https://en.wikipedia.org/wiki/Systemd
    # systems using systemd
#    if (dist[0].lower() in ["centos", "redhat"] and dist[1] >= 7
#        or dist[0].lower() in ["fedora"] and dist[1] >= 15
#        or dist[0].lower() in ["debian"] and dist[1] >= 8
#        or dist[0].lower() in ["ubuntu"] and dist[1] >= 15.04):
    if os.path.isfile("/usr/lib/systemd"):
        if command == "status":
            return_val = subprocess.call(["systemctl", "is-active",
                                          SYSTEMD_PREFIX + beamline])
        elif command == "start":
            return_val = subprocess.call(["systemctl", "start",
                                          SYSTEMD_PREFIX + beamline])

    # systems using init scripts
#    elif (dist[0].lower() in ["centos", "redhat"] and dist[1] < 7
#          or dist[0].lower() in ["fedora"] and dist[1] >= 15
#          or dist[0].lower() in ["debian"] and dist[1] >= 8
#          or dist[0].lower() in ["ubuntu"] and dist[1] < 15.04):
    elif os.path.isfile("/etc/init.d"):
        if command == "status":
            return_val = subprocess.call(["service", SERVICE_NAME, "status",
                                          beamline])
        elif command == "start":
            return_val = subprocess.call(["service", SERVICE_NAME, "start",
                                          beamline])

    return return_val


def get_ip_addr():
    p = subprocess.Popen(["hostname", "-I"],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # IPs are separated by a space and the output ends with a new line
    ip_list = p.stdout.readlines()[0].split(" ")[:-1]

    active_ips = []
    ip_complete = []

    for ip in ip_list:

        """
        # Version 1
        p = subprocess.Popen(["nslookup", ip],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        # output of nslookup 131.169.185.121:
        # Server:		131.169.40.200
        # Address:	        131.169.40.200#53
        #
        # 121.185.169.131.in-addr.arpa	name = zitpcx19282.desy.de.
        # only the last line is needed without the new line
        name_line = p.stdout.readlines()[-2]

        match_host = re.compile(r'[\w|.|-]+[\s]+name = ([\w|.|-]+).',
                                re.M | re.I)
        netgroup = []

        if match_host.match(name_line):
            print ip, match_host.match(name_line).group(1)
        """

        # Version 2
        try:
            ip_complete = socket.gethostbyaddr(ip)
        except:
            pass

        for i in ip_complete:
            if type(i) == list:
                active_ips += i
            elif i not in active_ips:
                active_ips.append(i)

    return active_ips


def get_config(conf):
    config = ConfigParser.RawConfigParser()

    with open(conf, 'r') as f:
        config_string = '[asection]\n' + f.read()

    try:
        config.read_string(config_string)
    except:
        config_fp = StringIO.StringIO(config_string)
        config.readfp(config_fp)

    return config


def get_bls_to_check():
    global CONFIG_PATH
    global CONFIG_PREFIX
    global CONFIG_POSTFIX

    files = [[os.path.join(CONFIG_PATH, f),
              f[len(CONFIG_PREFIX):-len(CONFIG_POSTFIX)]]
             for f in os.listdir(CONFIG_PATH)
             if f.startswith(CONFIG_PREFIX) and f.endswith(CONFIG_POSTFIX)]

    print ("Config files")
    print (files)

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


def remove_domain(x):
    return x.replace(".desy.de", "")


if __name__ == '__main__':

    # Get IP addresses
    active_ips = get_ip_addr()
    print ("Active Ips\n", active_ips)

    # mapping ip/hostname to beamline
    beamlines_to_activate, beamlines_to_deactivate = get_bls_to_check()
    print ("List of beamline receivers to check (activate)\n",
           beamlines_to_activate)
    print ("List of beamline receivers to check (deactivate)\n",
           beamlines_to_deactivate)

    # check if hidra runs for this beamline
    # and start it if that is not the case
    for bl in beamlines_to_deactivate:
        p = call_initsystem(bl, "status")

        if p != 0:
            print ("service", bl, "is not running")
        else:
            print ("service", bl, "is running, but has to be stopped")
            # stop service
#            p = call_initsystem(bl, "stop")

    # check if hidra runs for this beamline
    # and start it if that is not the case
    for bl in beamlines_to_activate:
        p = call_initsystem(bl, "status")

        if p != 0:
            print ("service", bl, "is not running, but has to be started")
            # start service
#            p = call_initsystem(bl, "start")
        else:
            print ("service", bl, "is running")
