from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import ldap

ldap_obj = ldap.initialize("ldap://it-ldap-slave.desy.de:1389")
res = ldap_obj.search_s(base="",
                        scope=ldap.SCOPE_SUBTREE,
                        filterstr="cn=a3p00-hosts",
                        attrlist=None)

print(res)

for dn, entry in res:
    hosts = []
    for group_str in entry["nisNetgroupTriple"]:
        # group_str is of the form '(asap3-mon.desy.de,-,)'
        host = group_str[1:-1].split(",")[0]
        hosts.append(host)

    print(hosts)
