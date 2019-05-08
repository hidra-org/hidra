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

"""Testing the task provider.
"""

# pylint: disable=missing-docstring

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
from multiprocessing import Process, freeze_support
import os
import pwd
import time
from shutil import copyfile
import zmq

from test_base import TestBase, create_dir
from datamanager import DataManager
from hidra import __version__

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataManager(TestBase):
    """Specification of tests to be performed for the TaskProvider.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataManager, self).setUp()

        # see https://docs.python.org/2/library/multiprocessing.html#windows
        freeze_support()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip
        # self.base_dir

        # Register context
        self.context = zmq.Context()
        self.com_socket = None
        self.fixed_recv_socket = None
        self.receiving_sockets = None

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self.local_target = os.path.join(self.base_dir, "data", "target")
        self.chunksize = 10485760  # = 1024*1024*10 = 10 MiB

        self.config["fixed_recv"] = 50100
        self.config["receiving_ports"] = [50102, 50103]

        fix_subdirs = ["commissioning/raw",
                       "commissioning/scratch_bl",
                       "current/raw",
                       "current/scratch_bl",
                       "local"]
        ports = self.config["ports"]
        self.datamanager_config = {
            "general": {
                "com_port": ports["com"],
                "control_pub_port": ports["control_pub"],
                "control_sub_port": ports["control_sub"],
                "request_fw_port": ports["request_fw"],
                "request_port": 50001,
                "ext_ip": self.ext_ip,
                "ldapuri": "it-ldap-slave.desy.de:1389",
                "log_name": "datamanager.log",
                "log_path": "/home/kuhnm/projects/hidra/logs",
                "log_size": 10485760,
                "onscreen": False,
                "procname": "hidra",
                "username": pwd.getpwuid(os.geteuid()).pw_name,
                "verbose": False,
                "whitelist": None
            },
            "eventdetector": {
                "type": "inotifyx_events",
                "eventdetector_port": 50003,
                "ext_data_port": 50101,
                "inotifyx_events": {
                    "monitored_dir": "/home/kuhnm/projects/hidra/data/source",
                    "fix_subdirs": fix_subdirs,
                    "create_fix_subdirs": False,
                    "monitored_events": {"IN_CLOSE_WRITE": [""]},
                    "use_cleanup": False,
                    "history_size": 0,
                    "action_time": 10,
                    "time_till_closed": 2,
                },
            },
            "datafetcher": {
                "type": "file_fetcher",
                "chunksize": self.chunksize,
                "data_stream_targets": [[self.con_ip,
                                         self.config["fixed_recv"]]],
                "local_target": "/home/kuhnm/projects/hidra/data/target",
                "use_data_stream": True,
                "number_of_streams": 1,
                "store_data": False,
                "remove_data": False,
                "cleaner_port": ports["cleaner"],
                "cleaner_trigger_port": ports["cleaner_trigger"],
                "confirmation_port": ports["confirmation"],
                "confirmation_resp_port": 50012,
                "datafetcher_port": 50010,
                "router_port": ports["router"],
                "status_check_port": 50050,
                "status_check_resp_port": 50011,
                "file_fetcher": {
                    "store_data": False,
                    "remove_data": False,
                    "fix_subdirs": fix_subdirs,
                }
            },
        }

        self.start = 100
        self.stop = 105

        self.appid = str(self.config["main_pid"]).encode("utf-8")

    def send_signal(self, signal, ports, prio=None):
        self.log.info("send_signal : %s, %s", signal, ports)

        send_message = [__version__.encode("utf-8"), self.appid, signal]

        targets = []
        if isinstance(ports, list):
            for port in ports:
                targets.append(["{}:{}".format(self.con_ip, port), prio, [""]])
        else:
            targets.append(["{}:{}".format(self.con_ip, ports), prio, [""]])

        targets_json = json.dumps(targets).encode("utf-8")
        send_message.append(targets_json)

        self.com_socket.send_multipart(send_message)

        received_message = self.com_socket.recv()
        self.log.info("Response : %s", received_message)
        self.assertEqual(received_message, signal)

    def test_datamanager(self):
        """Simulate incoming data and check if received events are correct.
        """

        self.fixed_recv_socket = self.set_up_recv_socket(
            self.config["fixed_recv"]
        )

        endpoints = self.config["endpoints"]

#        try:
#            self.com_socket = self.context.socket(zmq.REQ)
#            self.com_socket.connect(endpoints.com_con)
#            self.log.info("Start com_socket (connect): %s", endpoints.com_con)
#        except:
#            self.log.error("Failed to start com_socket (connect): %s"
#                           endpoints.com_con)
#            raise
        self.com_socket = self.start_socket(
            name="com_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint=endpoints.com_con
        )

        class Sender(DataManager):
            def __init__(self, **kwargs):
                DataManager.__init__(self, **kwargs)

                self.run()

        try:
            kwargs = dict(
                log_queue=self.log_queue,
                config=self.datamanager_config
            )
            sender = Process(target=Sender, kwargs=kwargs)
            sender.start()
        except:
            self.log.error("Exception when initiating DataManager",
                           exc_info=True)
            raise

        self.receiving_sockets = []
        for port in self.config["receiving_ports"]:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        self.send_signal(signal=b"START_STREAM",
                         ports=self.config["receiving_ports"][0],
                         prio=1)
        self.send_signal(signal=b"START_STREAM",
                         ports=self.config["receiving_ports"][1],
                         prio=0)

        self.log.debug("test receiver started")

        source_file = os.path.join(self.base_dir,
                                   "test",
                                   "test_files",
                                   "test_file.cbf")
        target_file_base = os.path.join(self.base_dir,
                                        "data",
                                        "source",
                                        "local",
                                        "raw")

        time.sleep(0.5)
        try:
            for i in range(self.start, self.stop):
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(i))
                self.log.debug("copy to %s", target_file)
                copyfile(source_file, target_file)

                time.sleep(1)

                recv_message = self.fixed_recv_socket.recv_multipart()

                if recv_message == ["ALIVE_TEST"]:
                    continue

                self.log.info("received fixed: %s",
                              json.loads(recv_message[0]))

                for sckt in self.receiving_sockets:
                    recv_message = sckt.recv_multipart()
                    self.log.info("received: %s", json.loads(recv_message[0]))

        except Exception as excp:
            self.log.error("Exception detected: %s", excp,
                           exc_info=True)
        finally:
            self.stop_socket(name="com_socket")
            self.stop_socket(name="fixed_recv_socket")

            for i, sckt in enumerate(self.receiving_sockets):
                self.stop_socket(name="receiving_socket{}".format(i),
                                 socket=sckt)

            sender.terminate()
            sender.join()

            for i in range(self.start, self.stop):
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(i))
                try:
                    os.remove(target_file)
                    self.log.debug("remove %s", target_file)
                except Exception:
                    pass

    def tearDown(self):
        self.context.destroy(0)

        super(TestDataManager, self).tearDown()
