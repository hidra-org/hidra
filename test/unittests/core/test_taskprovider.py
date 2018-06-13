"""Testing the task provider.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import socket
import tempfile
import time
import zmq
from collections import namedtuple
from shutil import copyfile
from multiprocessing import Process, freeze_support

from .__init__ import BASE_DIR
from test_base import TestBase, create_dir
from taskprovider import TaskProvider
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

ConStr = namedtuple(
    "con_str", [
        "control_bind",
        "control_con",
        "request_fw_bind",
        "request_fw_con",
        "router_bind",
        "router_con",
        # "cleaner_job_bind",
        # "cleaner_job_con",
        # "cleaner_trigger_bind",
        # "cleaner_trigger_con",
        # "confirm_bind",
        # "confirm_con"
    ]
)


def set_con_strs(ext_ip, con_ip, ipc_dir, main_pid, ports):
    """Sets the connection strings.

    Sets the connection strings  for the control, request forwarding and
    router socket.

    Args:
        ext_ip: IP to bind TCP connections to
        con_ip: IP to connect TCP connections to
        ipc_dir: Directory used for IPC connections
        main_pid: Process ID of the current process. Used to distinguish
                  different IPC connection.
        port: A dictionary giving the ports to open TCP connection on
              (only used on Windows).
    Returns:
        A namedtuple object ConStr with the entries:
            control_bind
            control_con
            request_fw_bind
            request_fw_con
            router_bind
            router_con
    """

    # determine socket connection strings
    if utils.is_windows():
        control_bind_str = "tcp://{}:{}".format(ext_ip, ports["control"])
        control_con_str = "tcp://{}:{}".format(con_ip, ports["control"])

        request_fw_bind_str = "tcp://{}:{}".format(ext_ip, ports["request_fw"])
        request_fw_con_str = "tcp://{}:{}".format(con_ip, ports["request_fw"])

        router_bind_str = "tcp://{}:{}".format(ext_ip, ports["router"])
        router_con_str = "tcp://{}:{}".format(con_ip, ports["router"])

    else:
        ipc_ip = "{}/{}".format(ipc_dir, main_pid)

        control_bind_str = "ipc://{}_{}".format(ipc_ip, "control")
        control_con_str = control_bind_str

        request_fw_bind_str = "ipc://{}:{}".format(ipc_ip, "request_fw")
        request_fw_con_str = request_fw_bind_str

        router_bind_str = "ipc://{}:{}".format(ipc_ip, "router")
        router_con_str = router_bind_str

    return ConStr(
        control_bind=control_bind_str,
        control_con=control_con_str,
        request_fw_bind=request_fw_bind_str,
        request_fw_con=request_fw_con_str,
        router_bind=router_bind_str,
        router_con=router_con_str,
        # cleaner_job_bind=job_bind_str,
        # cleaner_job_con=job_con_str,
        # cleaner_trigger_bind=trigger_bind_str,
        # cleaner_trigger_con=trigger_con_str,
        # confirm_bind=confirm_bind_str,
        # confirm_con=confirm_con_str
    )


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class RequestResponder():
    def __init__(self, config, log_queue, context=None):
        self.config = config

        # Send all logs to the main process
        self.log = utils.get_logger("RequestResponder", log_queue)

        self.context = context or zmq.Context.instance()

        con_str = self.config["con_strs"].request_fw_con
        self.request_fw_socket = self.context.socket(zmq.REP)
        self.request_fw_socket.bind(con_str)
        self.log.info("request_fw_socket started (bind) for '{}'"
                      .format(con_str))

        self.run()

    def run(self):
        hostname = socket.getfqdn()
        self.log.info("Start run")

        open_requests = [
            ['{}:6003'.format(hostname), 1, [".cbf"]],
            ['{}:6004'.format(hostname), 0, [".cbf"]]
        ]

        while True:
            request = self.request_fw_socket.recv_multipart()
            self.log.debug("Received request: {}".format(request))

            message = json.dumps(open_requests).encode("utf-8")
            self.request_fw_socket.send(message)
            self.log.debug("Answer: {}".format(open_requests))

    def __exit__(self):
        self.request_fw_socket.close(0)
        self.context.destroy()


class TestTaskProvider(TestBase):
    """Specification of tests to be performed for the TaskProvider.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestTaskProvider, self).setUp()

        # see https://docs.python.org/2/library/multiprocessing.html#windows
        freeze_support()

        self.context = zmq.Context.instance()

        main_pid = os.getpid()
        self.con_ip = socket.getfqdn()
        self.ext_ip = socket.gethostbyaddr(self.con_ip)[2][0]
        ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")

        create_dir(directory=ipc_dir, chmod=0o777)

        ports = {
            "control": "50005",
            "request_fw": "6001",
            "router": "7000"
        }

        con_strs = set_con_strs(ext_ip=self.ext_ip,
                                con_ip=self.con_ip,
                                ipc_dir=ipc_dir,
                                main_pid=main_pid,
                                ports=ports)

        self.config = {
            "con_strs": con_strs
        }

        self.task_provider_config = {
            "event_detector_type": "inotifyx_events",
            "monitored_dir": os.path.join(BASE_DIR, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {
                "IN_CLOSE_WRITE": [".tif", ".cbf"],
                "IN_MOVED_TO": [".log"]
            },
            "timeout": 0.1,
            "history_size": 0,
            "use_cleanup": False,
            "time_till_closed": 5,
            "action_time": 120
        }

        self.start = 100
        self.stop = 105

    def test_taskprovider(self):
        """Simulate incoming data and check if received events are correct.
        """

        con_strs = self.config["con_strs"]
        kwargs = dict(
            config=self.task_provider_config,
            control_con_str=con_strs.control_con,
            request_fw_con_str=con_strs.request_fw_con,
            router_bind_str=con_strs.router_bind,
            log_queue=self.log_queue
        )
        taskprovider_pr = Process(target=TaskProvider, kwargs=kwargs)
        taskprovider_pr.start()

        requestResponderPr = Process(target=RequestResponder,
                                     args=(self.config, self.log_queue))
        requestResponderPr.start()

        context = zmq.Context.instance()

        router_socket = context.socket(zmq.PULL)
        router_socket.connect(con_strs.router_con)
        self.log.info("router_socket connected to {}"
                      .format(con_strs.router_con))

        source_file = os.path.join(BASE_DIR, "test_file.cbf")
        target_file_base = os.path.join(BASE_DIR,
                                        "data",
                                        "source",
                                        "local",
                                        "raw")

        create_dir(target_file_base)

    #    time.sleep(5)
        try:
            for i in range(self.start, self.stop):
                time.sleep(0.5)
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(i))
                self.log.debug("copy to {}".format(target_file))
                copyfile(source_file, target_file)

                workload = router_socket.recv_multipart()
                self.log.info("next workload {}".format(workload))
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:

            requestResponderPr.terminate()
            taskprovider_pr.terminate()

            router_socket.close(0)

            for number in range(100, i):
                target_file = os.path.join(target_file_base,
                                           "{}.cbf".format(number))
                self.log.debug("remove {}".format(target_file))
                os.remove(target_file)

    def tearDown(self):
        self.context.destroy(0)

        super(TestTaskProvider, self).tearDown()
