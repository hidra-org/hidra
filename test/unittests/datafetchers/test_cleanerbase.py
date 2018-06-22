"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import os
import time
import shutil
import zmq
from multiprocessing import Process


from .__init__ import BASE_DIR
from .datafetcher_test_base import DataFetcherTestBase
from cleanerbase import CleanerBase
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataFetcher, self).setUp()

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
                except Exception:  # pylint: disable=broad-except
                    self.log.error("Unable to remove file {}"
                                   .format(source_file), exc_info=True)

        con_strs = self.config["con_strs"]

        # Instantiate cleaner as additional process
        con_strs = self.config["con_strs"]
        kwargs = dict(
            config=self.cleaner_config,
            log_queue=self.log_queue,
            job_bind_str=con_strs.cleaner_job_bind,
            cleaner_trigger_con_str=con_strs.cleaner_trigger_con,
            conf_con_str=con_strs.confirm_con,
            control_con_str=con_strs.control_sub_con
        )
        cleaner_pr = Process(target=Cleaner, kwargs=kwargs)
        cleaner_pr.start()

        # Set up datafetcher simulator
        job_socket = self.context.socket(zmq.PUSH)
        job_socket.connect(con_strs.cleaner_job_con)
        self.log.info("Start job_socket (connect): {}"
                      .format(con_strs.cleaner_job_con))

        # Set up receiver simulator
        confirmation_socket = self.context.socket(zmq.PUB)
        confirmation_socket.bind(con_strs.confirm_bind)
        self.log.info("Start confirmation_socket (bind): {}"
                      .format(con_strs.confirm_bind))

        # create control socket
        # control messages are not send over an forwarder, thus the
        # control_sub endpoint is used directly
        control_pub_socket = self.context.socket(zmq.PUB)
        control_pub_socket.bind(con_strs.control_sub_con)
        self.log.info("Start control_socket (bind): {}"
                      .format(con_strs.control_sub_con))

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
            self.log.debug("Sending control signal: EXIT")
            control_pub_socket.send_multipart([b"control", b"EXIT"])

            # give control signal time to be received
            time.sleep(1)

            self.log.debug("closing sockets")
            job_socket.close(0)
            confirmation_socket.close(0)

            control_pub_socket.close(0)

    def tearDown(self):
        super(TestDataFetcher, self).tearDown()
