# Copyright (C) 2015-2020  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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

"""
This module provides experimental utilities not exposed by default.
"""

from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import numpy as np


def zmq_msg_to_nparray(data, array_metadata):
    """ Deserialize numpy array that where sent via zmq """

    try:
        mem_view = bytes(memoryview(data))
        array = np.frombuffer(mem_view, dtype=array_metadata["dtype"])
    except ValueError:
        # python 2
        array = np.frombuffer(bytes(data), dtype=array_metadata["dtype"])
    return array.reshape(array_metadata["shape"])
