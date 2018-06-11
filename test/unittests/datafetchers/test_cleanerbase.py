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
from .test_datafetcher_base import (
    TestDataFetcherBase,
    create_dir,
    set_con_strs
)
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

        main_pid = os.getpid()
        con_ip = socket.getfqdn()
        ext_ip = socket.gethostbyaddr(con_ip)[2][0]

        ports = {
            "control": 50005,
            "cleaner": 50051,
            "cleaner_trigger": 50052,
            "confirmation_port": 50053,
        }

        con_strs = set_con_strs(ext_ip=ext_ip,
                                con_ip=con_ip,
                                ipc_dir=ipc_dir,
                                main_pid=main_pid,
                                ports=ports)

        # Set up config
        self.config = {
            "main_pid": main_pid,
            "con_strs": con_strs
        }

        # Set up config

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

    def test_cleaner(self):
        """Simulate a simple cleaner.
        """

        # Implement abstract class cleaner
        class Cleaner(CleanerBase):
            """A simple cleaner class
            """

            # pylint: disable=too-few-public-methods
            # Is reasonable in this case.

            def remove_element(self, base_path, source_file_id):
                """Removes the source file.
                """

                # generate file pat
                source_file = os.path.join(base_path, source_file_id)

                # remove file
                try:
                    os.remove(source_file)
                    self.log.info("Removing file '{}' ...success"
                                  .format(source_file))
                except Exception:
                    self.log.error("Unable to remove file {}"
                                   .format(source_file), exc_info=True)

        # Instantiate cleaner as additional process
        con_strs = self.config["con_strs"]
        kwargs = dict(
            config=self.cleaner_config,
            log_queue=self.log_queue,
            job_bind_str=con_strs.cleaner_job_bind,
            cleaner_trigger_con_str=con_strs.cleaner_trigger_con,
            conf_con_str=con_strs.confirm_con,
            control_con_str=con_strs.control_con,
            context=self.context
        )
        cleaner_pr = Process(target=Cleaner, kwargs=kwargs)
        cleaner_pr.start()

        # Set up datafetcher simulator
        job_socket = self.context.socket(zmq.PUSH)
        job_socket.connect(self.config["con_strs"].cleaner_job_con)
        self.log.info("Start job_socket (connect): {}"
                      .format(self.config["con_strs"].cleaner_job_con))

        # Set up receiver simulator
        confirmation_socket = self.context.socket(zmq.PUSH)
        confirmation_socket.bind(self.config["con_strs"].confirm_bind)
        self.log.info("Start confirmation_socket (bind): {}"
                      .format(self.config["con_strs"].confirm_bind))

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
