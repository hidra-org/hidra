import subprocess
import re
import socket
import os
import StringIO
try:
    import ConfigParser
except ImportError:
    # The ConfigParser module has been renamed to configparser in Python 3
    import configparser as ConfigParser


CONFIG_PATH = "/opt/hidra/conf"
CONFIG_PREFIX = "receiver_"
CONFIG_POSTFIX = ".conf"
SERVICE_PREFIX = "hidra-receiver@"


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


def get_service_list():
    global CONFIG_PATH
    global CONFIG_PREFIX
    global CONFIG_POSTFIX

    files = [[os.path.join(CONFIG_PATH, f), f[len(CONFIG_PREFIX):-len(CONFIG_POSTFIX)]]
             for f in os.listdir(CONFIG_PATH)
             if f.startswith(CONFIG_PREFIX) and f.endswith(CONFIG_POSTFIX) ]

    print "Config files"
    print files

    services_to_check = []
    for conf, bl in files:

        config = get_config(conf)

        ip = remove_domain(config.get("asection", "data_stream_ip"))

        for entry in active_ips:
	    # remove domain for easier host comparison
            if ip in remove_domain(entry):
                if SERVICE_PREFIX + bl not in services_to_check:
			services_to_check.append(SERVICE_PREFIX + bl)

    return services_to_check


def remove_domain(x):
    return x.replace(".desy.de", "")


if __name__ == '__main__':

    # Get IP addresses
    active_ips = get_ip_addr()
    print "Active Ips\n", active_ips

    # mapping ip/hostname to beamline
    services_to_check = get_service_list()
    print "List of beamline receivers to check\n", services_to_check

    # check if hidra runs for this beamline
    # and start it if that is not the case
    for s in services_to_check:
        p = subprocess.call(["systemctl", "status", s])

        if p != 0:
            print "service", s, "is not running"
            # start service
            p = subprocess.call(["systemctl", "start", s])
        else:
            print "service", s, "is running"
