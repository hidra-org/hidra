"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import os
import tempfile
import time
import shutil
import socket
import zmq
from multiprocessing import Process


from .__init__ import BASE_DIR
from .test_datafetcher_base import TestDataFetcherBase, create_dir
from cleanerbase import CleanerBase
import utils


class TestDataFetcher(TestDataFetcherBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataFetcher, self).setUp()

        # methods inherited from parent class
        # explicit definition here for better readability
        self._init_logging = super(TestDataFetcher, self)._init_logging

        self._init_logging()

        ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")

        create_dir(directory=ipc_dir, chmod=0o777)

        self.context = zmq.Context.instance()

        # Set up config
        self.config = {
            "ipc_dir": ipc_dir,
            "main_pid": os.getpid(),
            "cleaner_port": 50051,
            "confirmation_port": 50052,
            "control_port": "50005"
        }

    def test_cleaner(self):
        # Implement abstract class cleaner
        class Cleaner(CleanerBase):
            def remove_element(self, source_file):
                # remove file
                try:
                    os.remove(source_file)
                    self.log.info("Removing file '{}' ...success"
                                  .format(source_file))
                except:
                    self.log.error("Unable to remove file {}"
                                   .format(source_file), exc_info=True)

        con_ip = socket.getfqdn()
        ext_ip = socket.gethostbyaddr(con_ip)[2][0]

        # determine socket connection strings
        if utils.is_windows():
            job_con_str = "tcp://{}:{}".format(con_ip,
                                               self.config["cleaner_port"])
            job_bind_str = "tcp://{}:{}".format(ext_ip,
                                                self.config["cleaner_port"])

            control_con_str = "tcp://{}:{}".format(ext_ip,
                                                   self.config["control_port"])

            cleaner_trigger_con_str = (
                "tcp://{}:{}".format(ext_ip,
                                     self.config["cleaner_trigger_port"])
            )
        else:
            job_con_str = ("ipc://{}/{}_{}".format(self.config["ipc_dir"],
                                                   self.config["main_pid"],
                                                   "cleaner"))
            job_bind_str = job_con_str

            control_con_str = "ipc://{}/{}_{}".format(self.config["ipc_dir"],
                                                      self.config["main_pid"],
                                                      "control")

            cleaner_trigger_con_str = (
                "ipc://{}/{}_{}".format(self.config["ipc_dir"],
                                        self.config["main_pid"],
                                        "cleaner_trigger")
            )

        conf_con_str = "tcp://{}:{}".format(con_ip,
                                            self.config["confirmation_port"])
        conf_bind_str = "tcp://{}:{}".format(ext_ip,
                                             self.config["confirmation_port"])

        # Instantiate cleaner as additional process
        kwargs = dict(
            config=self.config,
            log_queue=self.log_queue,
            job_bind_str=job_bind_str,
            cleaner_trigger_con_str=cleaner_trigger_con_str,
            conf_bind_str=conf_bind_str,
            control_con_str=control_con_str,
            context=self.context
        )
        cleaner_pr = Process(target=Cleaner, kwargs=kwargs)
        cleaner_pr.start()

        # Set up datafetcher simulator
        job_socket = self.context.socket(zmq.PUSH)
        job_socket.connect(job_con_str)
        self.log.info("Start job_socket (connect): {}".format(job_con_str))

        # Set up receiver simulator
        confirmation_socket = self.context.socket(zmq.PUSH)
        confirmation_socket.connect(conf_con_str)
        self.log.info("Start confirmation_socket (connect): {}"
                      .format(conf_con_str))

        # to give init time to finish
        time.sleep(0.5)

        # Test cleaner
        source_file = os.path.join(BASE_DIR, "test_file.cbf")
        target_path = os.path.join(BASE_DIR, "data", "source", "local")

        try:
            for i in range(5):
                target_file = os.path.join(target_path, "{}.cbf".format(i))
                shutil.copyfile(source_file, target_file)

                target_file = target_file.encode("utf-8")
                file_id = utils.generate_sender_id(self.config["main_pid"])
                file_id = file_id.encode("utf-8")

                message = [target_file, file_id]
                self.log.debug("sending job {}".format(message))
                job_socket.send_multipart(message)
                self.log.debug("job sent {}".format(message))

                message = [file_id, target_file]
                confirmation_socket.send_multipart(message)
                self.log.debug("confirmation sent {}".format(message))
        except KeyboardInterrupt:
            pass
        finally:
            time.sleep(1)
            cleaner_pr.terminate()

    def tearDown(self):
        super(TestDataFetcher, self).tearDown()
