from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import ldap3 as ldap


def main():
    """ Connect to ldap server and resolve netgroup """

    ldap_servers = [
        "ldap://it-ldap-slave.desy.de:1389",
    ]

    servers = []
    for i in ldap_servers:
        servers.append(ldap.Server(i))

    servers = ldap.Server("ldap://it-ldap-slave.desy.de")
    con = ldap.Connection(servers)
    con.open()
    con.search(search_base="",
               search_filter="(cn=a3p00-hosts)",
               search_scope=ldap.SUBTREE,
               attributes=["nisNetgroupTriple"])

    if not con.response:
        print("Is not a netgroup, considering it as hostname")

    netgroup = []
    for entry in con.response:
        for group_str in entry["attributes"]["nisNetgroupTriple"]:
            # group_str is of the form '(asap3-mon.desy.de,-,)'
            host = group_str[1:-1].split(",")[0]
            netgroup.append(host)

    print(netgroup)


if __name__ == "__main__":
    main()
