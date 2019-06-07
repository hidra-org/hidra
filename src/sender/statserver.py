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

import json
import multiprocessing
import time
import zmq

from base_class import Base
import hidra.utils as utils


class StatServer(Base):
    def __init__(self, config, log_queue):
        super(StatServer, self).__init__()

        self.config = config
        self.log_queue = log_queue

        self.log = None
        self.keep_running = True
        self.stats = {"config": config}

        self.context = None
        self.stats_collect_socket = None
        self.stats_expose_socket = None

        self._setup()
        self.run()

    def _setup(self):
        self.log = utils.get_logger("StatsServer", self.log_queue)

        # --------------------------------------------------------------------
        # zmq setup                      
        # --------------------------------------------------------------------

        self.context = zmq.Context()

        endpoints = self.config["network"]["endpoints"]

        self.stats_collect_socket = self.start_socket(
            name="stats_collect_socket",
            sock_type=zmq.PULL,
            sock_con="bind",
            endpoint=endpoints.stats_collect_bind
        )

        # socket to get control signals from
        self.control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=endpoints.control_sub_con
        )
        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, u"control")

#        self.stats_expose_socket = self.start_socket(
#            name="stats_expose_socket",
#            sock_type=zmq.REP,
#            sock_con="connect",
#            endpoint=endpoints.stats_expose_con
#        )

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)
        self.poller.register(self.stats_collect_socket, zmq.POLLIN)

    def run(self):
        while self.keep_running:
            socks = dict(self.poller.poll())

            # ----------------------------------------------------------------
            # incoming stats
            # ----------------------------------------------------------------
            if (self.stats_collect_socket in socks
                    and socks[self.stats_collect_socket] == zmq.POLLIN):

                new = self.stats_collect_socket.recv()
                new = json.loads(new.decode())
                self.log.debug("received: %s", new)

                if new == [b"STOP"]:
                    break

                self._update(*new)

            # ----------------------------------------------------------------
            # control commands from internal
            # ----------------------------------------------------------------
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):

                # the exit signal should become effective
                if self.check_control_signal():
                    break

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

        self.stop_socket(name="stats_collect_socket")
        self.stop_socket(name="control_socket")
#        self.stop_socket(name="stats_expose_socket")

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()