# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""Package providing the event detector test classes.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import sys

CURRENT_DIR = os.path.realpath(__file__)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))

SENDER_DIR = os.path.join(BASE_DIR, "src", "hidra", "sender")
EVENT_DETECTOR_DIR = os.path.join(SENDER_DIR, "eventdetectors")
DATAFETCHER_DIR = os.path.join(SENDER_DIR, "datafetchers")
API_DIR = os.path.join(BASE_DIR, "src", "api", "python")
RECEIVER_DIR = os.path.join(BASE_DIR, "src", "hidra", "receiver")

if SENDER_DIR not in sys.path:
    sys.path.insert(0, SENDER_DIR)

if EVENT_DETECTOR_DIR not in sys.path:
    sys.path.insert(0, EVENT_DETECTOR_DIR)

if DATAFETCHER_DIR not in sys.path:
    sys.path.insert(0, DATAFETCHER_DIR)

if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

if RECEIVER_DIR not in sys.path:
    sys.path.insert(0, RECEIVER_DIR)
