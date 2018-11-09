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

"""Testing the file_fetcher data fetcher.
"""

# pylint: disable=redefined-variable-type

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from multiprocessing import Process
import os
import time
import shutil
import zmq

from cleanerbase import CleanerBase
import utils
from .__init__ import BASE_DIR
from .datafetcher_test_base import DataFetcherTestBase

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
                    self.log.info("Removing file '%s' ...success", source_file)
                except Exception:  # pylint: disable=broad-except
                    self.log.error("Unable to remove file %s", source_file,
                                   exc_info=True)

        endpoints = self.config["endpoints"]

        # Instantiate cleaner as additional process
        kwargs = dict(
            config=self.cleaner_config,
            log_queue=self.log_queue,
            endpoints=endpoints
        )
        cleaner_pr = Process(target=Cleaner, kwargs=kwargs)
        cleaner_pr.start()

        # Set up datafetcher simulator
        job_socket = self.start_socket(
            name="job_socket",
            sock_type=zmq.PUSH,
            sock_con="connect",
            endpoint=endpoints.cleaner_job_con
        )

        # Set up receiver simulator
        confirmation_socket = self.start_socket(
            name="confirmation_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            endpoint=endpoints.confirm_bind
        )

        # create control socket
        # control messages are not send over an forwarder, thus the
        # control_sub endpoint is used directly
        control_pub_socket = self.start_socket(
            name="control_pub_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            endpoint=endpoints.control_sub_bind
        )

        # to give init time to finish
        time.sleep(0.5)

        # Test cleaner
        source_file = os.path.join(BASE_DIR,
                                   "test",
                                   "test_files",
                                   "test_file.cbf")
        target_path = os.path.join(BASE_DIR, "data", "source", "local")

        try:
            for i in range(5):
                target_file = os.path.join(target_path, "{}.cbf".format(i))
                shutil.copyfile(source_file, target_file)

                target_file = target_file.encode("utf-8")
                file_id = utils.generate_sender_id(self.config["main_pid"])
                file_id = file_id.encode("utf-8")
                n_chunks = str(1)

                message = [target_file, file_id, n_chunks]
                self.log.debug("sending job %s", message)
                job_socket.send_multipart(message)
                self.log.debug("job sent %s", message)

                message = [file_id, target_file]
                confirmation_socket.send_multipart(message)
                self.log.debug("confirmation sent %s", message)
        except KeyboardInterrupt:
            pass
        finally:
            self.log.debug("Sending control signal: EXIT")
            control_pub_socket.send_multipart([b"control", b"EXIT"])

            # give control signal time to be received
            time.sleep(1)

            job_socket = self.stop_socket(name="job_socket", socket=job_socket)
            confirmation_socket = self.stop_socket(name="confirmation_socket",
                                                   socket=confirmation_socket)
            control_pub_socket = self.stop_socket(name="control_pub_socket",
                                                  socket=control_pub_socket)

    def tearDown(self):
        super(TestDataFetcher, self).tearDown()
