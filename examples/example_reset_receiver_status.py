from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import socket

import __init__  # noqa E401
from hidra.control import ReceiverControl

host = socket.getfqdn()
control = ReceiverControl(host)

status = control.get_status()
print("status before reset", status)

control.reset_status()

status = control.get_status()
print("status after reset", status)
