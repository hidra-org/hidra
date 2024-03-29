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

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import json
import os
import setproctitle
import zmq

from base_class import Base
import hidra.utils as utils


class StatServer(Base):
    """
    Handler for status, configuration and statistics communication to the
    outside.
    """

    def __init__(self, config, log_queue, log_level, stop_request):
        super().__init__()

        self.config = config
        self.log_queue = log_queue
        self.log_level = log_level
        self.stop_request = stop_request

        self.log = None
        self.stats = {"config": config}

        self.ipc_dir_umask = 0o001

        self.context = None
        self.poller = None
        self.stats_collect_socket = None
        self.stats_expose_socket = None

    def _setup(self):
        self.log = utils.get_logger(self.__class__.__name__,
                                    queue=self.log_queue,
                                    log_level=self.log_level)

        # --------------------------------------------------------------------
        # zmq setup
        # --------------------------------------------------------------------

        self.context = zmq.Context()

        endpoints = self.config["network"]["endpoints"]

        # to enable communication between the StatServer and
        # hidra-control running as different users
        # this has to be called even if the ipc dir was already created by
        # another hidra instance
        old_umask = os.umask(self.ipc_dir_umask)
        self.log.info("Changed umask on ipc_dir to %s", self.ipc_dir_umask)

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

        self.stats_expose_socket = self.start_socket(
            name="stats_expose_socket",
            sock_type=zmq.REP,
            sock_con="bind",
            endpoint=endpoints.stats_expose_bind
        )

        # revert change of umask after ipc dir was created to not interfere
        # with anything else (e.g. data files stored)
        os.umask(old_umask)
        self.log.info("Changed back to old umask %s", old_umask)

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)
        self.poller.register(self.stats_collect_socket, zmq.POLLIN)
        self.poller.register(self.stats_expose_socket, zmq.POLLIN)

    def run(self):
        """Collect stats from and exposes them.
        """
        self._setup()

        try:
            self._run()
        finally:
            self.stop()

    def _run(self):

        while not self.stop_request.is_set():
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
            # external requests
            # ----------------------------------------------------------------
            if (self.stats_expose_socket in socks
                    and socks[self.stats_expose_socket] == zmq.POLLIN):

                key = self.stats_expose_socket.recv()
                key = json.loads(key.decode())
                self.log.debug("key=%s", key)

                try:
                    answer = self.stats[key]
                except KeyError:
                    self.log.error("Key '%s' not found in stats", key)
                    answer = "ERROR"

                self.log.debug("Answer to stats_expose_socket: %s", answer)
                self.stats_expose_socket.send(json.dumps(answer).encode())

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
            conf = self.stats["config"]
            for i in param[:-1]:
                conf = conf[i]
            conf[param[-1]] = value

        else:
            self.stats[param] = value

    def _get(self, param):
        return self.stats[param]

    def stop(self):
        """Stop and clean up.
        """
        self.stop_request.set()

        self.stop_socket(name="stats_collect_socket")
        self.stop_socket(name="control_socket")
        self.stop_socket(name="stats_expose_socket")

        if self.context is not None:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None


def run_statserver(**kwargs):
    """ Wrapper to run in a process or thread"""
    procname = kwargs.pop("procname", None)
    if procname:
        setproctitle.setproctitle(procname)
    proc = StatServer(**kwargs)
    proc.run()
