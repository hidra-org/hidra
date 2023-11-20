
from ctypes import *

def get_hostname(netgroup):

    libc = CDLL("libc.so.6")
    if (libc != None):
        pass
    else:
        libc = CDLL("libc.so.7")

 
    hosts = None
    if libc.setnetgrent(netgroup):
        # returns 1 if successful and 0 otherwise
        host = c_char_p()
        user = c_char_p()
        domain = c_char_p()
        
        hosts = []
        while libc.getnetgrent(byref(host), byref(user), byref(domain)):
            if host:
                # check is host is not a NULL pointer
                hosts.append(host.value)
    
    return(hosts)


hosts = get_hostname(b"a3p62-hosts")
print(hosts)
print("done")