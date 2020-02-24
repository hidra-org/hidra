from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import ldap3 as ldap

#server = ldap.Server("ldap://it-ldap-slave.desy.de:1389")
server = ldap.Server("ldap://it-ldap-slave.desy.de")
c = ldap.Connection(server)
c.open()
c.search(search_base="",
         search_filter="(cn=a3p00-hosts)",
         search_scope=ldap.SUBTREE,
         attributes=["nisNetgroupTriple"])

if not c.response:
    print("Is not a netgroup, considering it as hostname")

netgroup = []
for entry in c.response:
    for group_str in entry["attributes"]["nisNetgroupTriple"]:
        # group_str is of the form '(asap3-mon.desy.de,-,)'
        host = group_str[1:-1].split(",")[0]
        netgroup.append(host)

print(netgroup)
