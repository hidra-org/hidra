from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import socket

import __init__  # noqa E401
from hidra.control import ReceiverControl

#host = "asap3-p00"
host = socket.getfqdn()
control = ReceiverControl(host)

status = control.get_status()
print("status", status)
