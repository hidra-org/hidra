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

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import os

try:
    import unittest.mock as mock
except ImportError:
    # for python2
    import mock

import hidra.utils as utils

from datafetchers.datafetcherbase import DataFetcherBase
from .datafetcher_test_base import DataFetcherTestBase


__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):
    """ A base implementation to be able to test the base funktionality """

    def __init__(self, datafetcher_base_config):
        DataFetcherBase.__init__(self, datafetcher_base_config,
                                 name=__name__)

    def get_metadata(self, targets, metadata):
        self.source_file = "my_source_file"
        self.target_file = "my_target_file"

    def send_data(self, targets, metadata, open_connections):
        pass

    def finish(self, targets, metadata, open_connections):
        pass

    def stop(self):
        pass


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super().setUp()

        self.datafetcher = None

        # Set up config
        self.module_name = "datafetcherbase"
        self.df_base_config["config"] = {
            "network": {
                "endpoints": None,
                "main_pid": self.config["main_pid"]
            },
            "datafetcher": {
                "type": self.module_name,
                "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
                "local_target": None,
                "remove_data": False,
                "use_cleaner": False,
                self.module_name: {
                }
            }
        }

    def test_send_to_targets(self):
        """Simulate that the send_to_target method does not stop iteration"""

        self.datafetcher = DataFetcher(self.df_base_config)

        # Test data fetcher
        metadata = {
            "source_path": os.path.join(self.base_dir, "data", "source"),
            "relative_path": os.sep + "local",
            "filename": "100.cbf"
        }
        payload = None
        chunk_number = None

        ports = ["6005", "6006"]

        # no-prio targets
        targets = [
            ["{}:{}".format(self.con_ip, ports[0]), 1, "metadata"],
            ["{}:{}".format(self.con_ip, ports[1]), 1, "metadata"]
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: %s",
                       open_connections)

        with mock.patch.object(DataFetcher, "_send_data") as mocked_send_data:
            with mock.patch.object(DataFetcher, "_open_socket"):
                # simulate that first sending fails but second works
                mocked_send_data.side_effect = [Exception, None]
                with self.assertRaises(utils.DataError):
                    self.datafetcher.send_to_targets(
                        targets,
                        open_connections,
                        metadata,
                        payload,
                        chunk_number,
                        timeout=-1
                    )
                self.assertTrue(mocked_send_data.call_count == 2)

        self.log.debug("open_connections after function call: %s",
                       open_connections)
