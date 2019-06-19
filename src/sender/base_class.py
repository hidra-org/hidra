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
This module implements the base class from which all classes of the sender
inherit from.
"""


from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import json
import multiprocessing
import zmq

import _environment  # noqa F401 # pylint: disable=unused-import
import hidra.utils as utils
from hidra.utils import WrongConfiguration


# ------------------------------ #
#  Base class for ZMQ handling   #
# ------------------------------ #

class Base(object):
    """
    Implementation of the sender base class.
    """

    def __init__(self):
        # make the class cooperative for multiple inheritance
        super().__init__()

        self.log = None
        self.context = None

        self.config_all = {}
        self.required_params_base = {}
        self.required_params_dep = {}
        self.config_reduced = {}

        self.stats_collect_socket = None
        self.control_socket = None

    def _base_check(self, module_class, check_dep=True):
        """
        eg. module_class is "eventdetector"
        """

        # Check format of base config
        self.config_reduced = self._check_config_base(
            config=self.config_all,
            required_params=self.required_params_base
        )

        # Check format of dependent config
        if check_dep:
            self.required_params_dep = {
                module_class: [self.config_all[module_class]["type"]]
            }

            config_reduced_dep = self._check_config_base(
                config=self.config_all,
                required_params=[self.required_params_base,
                                 self.required_params_dep],
            )

            self.config_reduced.update(config_reduced_dep)
        else:
            self.required_params_dep = {}

    def _check_config_base(self, config, required_params):
        """
        Check a list of required parameters (in order) and if successful
        combines the results.

        Args:
            config (dict): The configuration dictionary to test.
            required_params (dict or list): The parameters that should be
                                            contained.

        Returns:
            A dictionary containing the values for the necessary parameters
            only.

        Raises:
            WrongConfiguration: The configuration has missing or
                                wrong parameters.
        """

        config_reduced = {}

        if isinstance(required_params, dict):
            required_params = [required_params]
        elif not isinstance(required_params, list):
            raise WrongConfiguration("Wrong input, required_params has to "
                                     "be list or dict.")

        for params in required_params:
            check_passed, config_part = utils.check_config(
                required_params=params,
                config=config,
                log=self.log,
                serialize=False
            )

            if check_passed:
                config_reduced.update(config_part)
            else:
                raise WrongConfiguration(
                    "The configuration has missing or wrong parameters."
                )

        return config_reduced

    def setup_stats_collection(self):
        """Sets up communication to stats server.
        """
        endpoints = self.config["network"]["endpoints"]

        self.stats_collect_socket = self.start_socket(
            name="stats_collect_socket",
            sock_type=zmq.PUSH,
            sock_con="connect",
            endpoint=endpoints.stats_collect_con
        )

    def stats_config(self):
        """Mapping for stats server.
        """
        return {}

    def update_stats(self, name, value):
        """Send values to update to stats server
        """

        if self.stats_collect_socket is None:
            return

        try:
            stat_name = self.stats_config()[name]
            self.log.debug("Update(%s): %s", name, value)

            msg = json.dumps([stat_name, value]).encode()
            self.stats_collect_socket.send(msg)
        except Exception:
            self.log.error("Error when sending stats for %s", name,
                           exc_info=True)
            self.log.debug("value=%s", value)
            self.log.debug("msg=%s", msg)

    def start_socket(self,
                     name,
                     sock_type,
                     sock_con,
                     endpoint,
                     random_port=None,
                     message=None):
        """Wrapper of start_socket

        Args:
            name: The name of the socket (used in log messages).
            sock_type: ZMQ socket type (e.g. zmq.PULL).
            sock_con: ZMQ binding type (connect or bind).
            endpoint: ZMQ endpoint to connect to.
            random_port (optional): Use zmq bind_to_random_port option.
            message (optional): Wording to be used in the message
                                (default: Start).

        Returns:
            The ZMQ socket with the specified properties.
        """

        socket, port = utils.start_socket(
            name=name,
            sock_type=sock_type,
            sock_con=sock_con,
            endpoint=endpoint,
            context=self.context,
            log=self.log,
            random_port=random_port,
            message=message,
        )

        if port is not None:
            self.update_stats(name, port)

        return socket

    def stop_socket(self, name, socket=None):
        """Wrapper for stop_socket.

        Args:
            name: The name of the socket (used in log messages).
            socket (optional): The ZMQ socket to be closed.


        Returns:
            None if the socket was closed.
        """

        # use the class attribute
        if socket is None:
            try:
                socket = getattr(self, name)
                use_class_attribute = True
            except AttributeError:
                # socket does not exist thus cannot be removed
                return None
        else:
            use_class_attribute = False

        return_socket = utils.stop_socket(name=name,
                                          socket=socket,
                                          log=self.log)

        # class attributes are set directly
        if use_class_attribute:
            setattr(self, name, return_socket)
        else:
            return return_socket

    def check_control_signal(self):
        """Check the control socket for signals and react accordingly.

        Returns:
            A boolean indicating if the class should be stopped or not
            (True means stop).
        """

        stop_flag = False

        try:
            message = self.control_socket.recv_multipart()
            self.log.debug("Control signal received: message = %s", message)
        except Exception:
            self.log.error("Receiving control signal...failed",
                           exc_info=True)
            return stop_flag

        # remove subsription topic
        del message[0]

        self._forward_control_signal(message)

        if message[0] == b"EXIT":
            self.log.debug("Received %s signal.", message[0])
            self._react_to_exit_signal()
            stop_flag = True

        elif message[0] == b"CLOSE_SOCKETS":
            self.log.debug("Received %s signal", message[0])
            self._react_to_close_sockets_signal(message)

        elif message[0] == b"SLEEP":
            self.log.debug("Received %s signal", message[0])
            self._react_to_sleep_signal(message)

        elif message[0] == b"WAKEUP":
            self.log.debug("Received %s signal without sleeping", message[0])
            self._react_to_wakeup_signal(message)
        else:
            self.log.error("Unhandled control signal received: %s",
                           message)

        return stop_flag

    def _forward_control_signal(self, message):
        """If the control signal has to trigger additional action.

        For some child classes additional action has to take place when a
        control signal is received. (They override this method then)
        """
        pass

    def _react_to_close_sockets_signal(self, message):
        """Action to take place when close sockets signal received.

        For some child classes action has to take place when the close socket
        signal is received. (They override this method then)
        """
        pass

    def _react_to_sleep_signal(self, message):
        """Action to take place when sleep signal received.

        By default processes go to sleep when the sleep signal is received till
        they are requested to wakeup or shut down.
        For some child classes another action has to take place. (They
        override this method then)
        """
        stop_flag = False

        # if there are problems on the receiving side no data
        # should be processed till the problem is solved
        while True:

            try:
                message = self.control_socket.recv_multipart()
            except KeyboardInterrupt:
                self.log.error("Receiving control signal..."
                               "failed due to KeyboardInterrupt")
                stop_flag = True
                break

            except Exception:
                self.log.error("Receiving control signal...failed",
                               exc_info=True)
                continue

            # remove subsription topic
            del message[0]

            if message[0] == b"SLEEP":
                self.log.debug("Received %s signal while sleeping.",
                               message[0])
                continue

            elif message[0] == b"WAKEUP":
                self.log.debug("Received %s signal", message[0])
                self._react_to_wakeup_signal(message)

                # Wake up from sleeping
                break

            elif message[0] == b"EXIT":
                self.log.debug("Received %s signal while sleeping.",
                               message[0])
                stop_flag = True
                break

            elif message[0] == b"CLOSE_SOCKETS":
                self.log.debug("Received %s signal while sleeping",
                               message[0])
                self._react_to_close_sockets_signal(message)
                continue

            else:
                self.log.error("Unhandled control signal received: %s",
                               message)

        return stop_flag

    def _react_to_wakeup_signal(self, message):
        """Action to take place when waking up.

        For some child classes action has to take place when a wakeup call is
        received. (They override this method then)
        """
        pass

    def _react_to_exit_signal(self):
        """Action to take place when exit signal received.

        For some child classes action has to take place when an exit signal is
        received. (They override this method then)
        """
        pass

    def stop(self):
        """Stop sockets and clean up.
        """
        if self.stats_collect_socket is not None:
            self.stop_socket(name="stats_collect_socket")
