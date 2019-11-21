from __future__ import print_function

import socket
import subprocess
import re


def execute_ldapsearch(ldap_cn):

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


def main():
    beamline = "p01"
    netgroup_name = "a3{0}-hosts".format(beamline)
    hostname = socket.getfqdn()

    netgroup = execute_ldapsearch(netgroup_name)
    print(netgroup)

    print("Host", hostname, "is in netgroup:", hostname in netgroup)


if __name__ == "__main__":
    main()
