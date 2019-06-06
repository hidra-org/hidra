#!/usr/bin/env python

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

"""
This module implements the data dispatcher.
"""

import multiprocessing
import time

from base_class import Base
import hidra.utils as utils


class StatServer(Base, multiprocessing.Process):
    def __init__(self, log_queue):
        super(StatServer, self).__init__()
        
        self.log = utils.get_logger("StatsServer", log_queue)

        self.keep_running = True
        self.stats = {}

    def run(self):
        while self.keep_running:
            new = self.stats_queue.get()

            if new =="STOP":
                break

            self._update(*new)

    def _update(self, param, value):
        self.log.debug("Update: %s", param)

        if isinstance(param, list):
            s = self.stats["config"]
            for i in param[:-1]:
                s = s[i]
            s[param[-1]] = value

        else:
            self.stats[param] = value

    def _get(self, param, value):
        self.stats[param]

    def stop(self):
        self.keep_running = False
        self.stats_queue.put("STOP")
