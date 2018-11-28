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

"""Testing the inotifyx_events event detector.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
import time
import logging
from shutil import copyfile

from inotify_utils import get_event_message, CleanUp
from .eventdetector_test_base import EventDetectorTestBase, create_dir

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestInotifyUtils(EventDetectorTestBase):
    """Specification of tests to be performed for the loaded EventDetecor.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestInotifyUtils, self).setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        source_path = "/ramdisk/source_dir/raw"
        relative_path = ""
        #relative_path = "subdir/testdir"

        self.abs_file_path = os.path.normpath(os.path.join(source_path, relative_path))
        print(self.abs_file_path)
        self.filename = "test_file.cbf"
        self.paths = ["/ramdisk/source_dir/raw", "/ramdisk/source_dir/scratch"]

        self.expected_result = {
            u'source_path': source_path,
            u'relative_path': relative_path,
            u'filename': self.filename
        }

    def test_get_event_message(self):
        """Simulate incoming data and check if received events are correct.
        """

        abs_file_path = "/ramdisk/source_dir/raw/subdir/testdir"
        filename = "test_file.cbf"
        paths = ["/ramdisk/source_dir/raw", "/ramdisk/source_dir/scratch"]

        expected_result = {
            u'source_path': "/ramdisk/source_dir/raw",
            u'relative_path': "subdir/testdir",
            u'filename': filename
        }

        event_message = get_event_message(abs_file_path,
                                          filename,
                                          paths)
        try:
            self.assertDictEqual(event_message, expected_result)
        except AssertionError:
            self.log.debug("event_message %s", event_message)
            raise

    def test_get_event_message_no_rel_path(self):
        """Simulate incoming data and check if received events are correct.
        """

        abs_file_path = "/ramdisk/source_dir/raw"
        filename = "test_file.cbf"
        paths = ["/ramdisk/source_dir/raw", "/ramdisk/source_dir/scratch"]

        expected_result = {
            u'source_path': "/ramdisk/source_dir/raw",
            u'relative_path': ".",  # comes from os.path.normpath
            u'filename': filename
        }

        event_message = get_event_message(abs_file_path,
                                          filename,
                                          paths)
        try:
            self.assertDictEqual(event_message, expected_result)
        except AssertionError:
            self.log.debug("event_message %s", event_message)
            raise

    def test_get_event_message_except(self):
        """Simulate incoming data and check if received events are correct.
        """

        abs_file_path = "/ramdisk/source_dir"
        filename = "test_file.cbf"
        paths = ["/ramdisk/source_dir/raw", "/ramdisk/source_dir/scratch"]

        with self.assertRaises(Exception):
            event_message = get_event_message(abs_file_path,
                                              filename,
                                              paths)

    def tearDown(self):
        super(TestInotifyUtils, self).tearDown()
