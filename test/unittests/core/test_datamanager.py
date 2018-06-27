"""Testing the task provider.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import time
import zmq
from multiprocessing import Process, freeze_support
from shutil import copyfile

from .__init__ import BASE_DIR
from test_base import TestBase, create_dir
from datamanager import DataManager
from _version import __version__

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

        # Register context
        self.context = zmq.Context()

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self.local_target = os.path.join(BASE_DIR, "data", "target")
        self.chunksize = 10485760  # = 1024*1024*10 = 10 MiB

        self.config["fixed_recv"] = 50100
        self.config["receiving_ports"] = [50102, 50103]

        self.datamanager_config = {
            'action_time': 10,
            'chunksize': self.chunksize,
            'cleaner_port': self.config["ports"]["cleaner"],
            'cleaner_trigger_port': self.config["ports"]["cleaner_trigger"],
            'com_port': self.config["ports"]["com"],
            'config_file': '/home/kuhnm/projects/hidra/conf/datamanager.conf',
            'confirmation_port': self.config["ports"]["confirmation"],
            'confirmation_resp_port': 50012,
            'control_pub_port': self.config["ports"]["control_pub"],
            'control_sub_port': self.config["ports"]["control_sub"],
            'create_fix_subdirs': False,
            'data_fetcher_port': 50010,
            'data_fetcher_type': 'file_fetcher',
            'data_stream_targets': [[self.con_ip, self.config["fixed_recv"]]],
            'det_api_version': '1.6.0',
            'det_ip': 'asap3-mon',
            'event_det_port': 50003,
            'event_detector_type': 'inotifyx_events',
            'ext_data_port': 50101,
            'ext_ip': self.ext_ip,
            'fix_subdirs': ['commissioning/raw',
                            'commissioning/scratch_bl',
                            'current/raw',
                            'current/scratch_bl',
                            'local'],
            'history_size': 0,
            'ldapuri': 'it-ldap-slave.desy.de:1389',
            'local_target': '/home/kuhnm/projects/hidra/data/target',
            'log_file': '/home/kuhnm/projects/hidra/logs/datamanager.log',
            'log_name': 'datamanager.log',
            'log_path': '/home/kuhnm/projects/hidra/logs',
            'log_size': 10485760,
            'monitored_dir': '/home/kuhnm/projects/hidra/data/source',
            'monitored_events': {'IN_CLOSE_WRITE': ['']},
            'number_of_streams': 1,
            'onscreen': False,
            'procname': 'hidra',
            'remove_data': False,
            'request_fw_port': self.config["ports"]["request_fw"],
            'request_port': 50001,
            'router_port': self.config["ports"]["router"],
            'status_check_port': 50050,
            'status_check_resp_port': 50011,
            'store_data': False,
            'time_till_closed': 2,
            'use_cleanup': False,
            'use_data_stream': True,
            'username': 'hidrauser',
            'verbose': False,
            'whitelist': None
        }

        self.start = 100
        self.stop = 105

    def send_signal(self, signal, ports, prio=None):
        self.log.info("send_signal : {}, {}".format(signal, ports))

        send_message = [__version__, signal]

        targets = []
        if type(ports) == list:
            for port in ports:
                targets.append(["{}:{}".format(self.con_ip, port), prio, [""]])
        else:
            targets.append(["{}:{}".format(self.con_ip, ports), prio, [""]])

        targets = json.dumps(targets).encode("utf-8")
        send_message.append(targets)

        self.com_socket.send_multipart(send_message)

        received_message = self.com_socket.recv()
        self.log.info("Responce : {}".format(received_message))

    def test_datamanager(self):
        """Simulate incoming data and check if received events are correct.
        """

        self.fixed_recv_socket = self.set_up_recv_socket(
            self.config["fixed_recv"]
        )

        endpoints = self.config["endpoints"]

        try:
            self.com_socket = self.context.socket(zmq.REQ)
            self.com_socket.connect(endpoints.com_con)
            self.log.info("Start com_socket (connect): {}"
                          .format(endpoints.com_con))
        except:
            self.log.error("Failed to start com_socket (connect): {}"
                           .format(endpoints.com_con))
            raise

        try:
            kwargs = dict(
                log_queue=self.log_queue,
                config=self.datamanager_config
            )
            sender = Process(target=DataManager, kwargs=kwargs)
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

        source_file = os.path.join(BASE_DIR, "test_file.cbf")
        target_file_base = os.path.join(BASE_DIR,
                                        "data",
                                        "source",
                                        "local",
                                        "raw")

        time.sleep(0.5)
        try:
            for i in range(self.start, self.stop):
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(i))
                self.log.debug("copy to {}".format(target_file))
                copyfile(source_file, target_file)

                time.sleep(1)

                recv_message = self.fixed_recv_socket.recv_multipart()

                if recv_message == ["ALIVE_TEST"]:
                    continue

                self.log.info("received fixed: {}"
                              .format(json.loads(recv_message[0])))

                for sckt in self.receiving_sockets:
                    recv_message = sckt.recv_multipart()
                    self.log.info("received: {}"
                                  .format(json.loads(recv_message[0])))

        except Exception as e:
            self.log.error("Exception detected: {}".format(e),
                           exc_info=True)
        finally:
            self.com_socket.close(0)
            self.fixed_recv_socket.close(0)

            for sckt in self.receiving_sockets:
                sckt.close(0)

            sender.terminate()
#            sender.join()

            for i in range(self.start, self.stop):
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(i))
                try:
                    os.remove(target_file)
                    self.log.debug("remove {}".format(target_file))
                except:
                    pass

    def tearDown(self):
        self.context.destroy(0)

        super(TestDataManager, self).tearDown()
