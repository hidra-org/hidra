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

"""Testing the zmq_events event detector.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

from test_base import TestBase
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestUtilsConfig(TestBase):
    """Specification of tests to be performed for the utils_config module.
    """

    def setUp(self):
        super().setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        self.fix_subdirs = [
            'commissioning/raw',
            'commissioning/scratch_bl',
            'current/raw',
            'current/scratch_bl',
            'local'
        ]

    def test_map_config_format(self):
        flat_config = {
            'datafetcher': {
                'chunksize': 10485760,
                'cleaner_port': 50051,
                'cleaner_trigger_port': 50052,
                'confirmation_port': 50053,
                'confirmation_resp_port': 50012,
                'data_fetcher_port': 50010,
                'router_port': 50004,
                'status_check_port': 50050,
                'status_check_resp_port': 50011
            },
            'eventdetector': {
                'dirs_not_to_create': [],
                'event_det_port': 50003,
                'ext_data_port': 50101
            },
            'general': {
                'com_port': 50000,
                'control_pub_port': 50005,
                'control_sub_port': 50006,
                'ldapuri': 'it-ldap-slave.desy.de:1389',
                'log_size': 10485760,
                'request_fw_port': 50002,
                'request_port': 50001
            }
        }

        expected_result = {
            'datafetcher': {
                'chunksize': 10485760,
                'cleaner_port': 50051,
                'cleaner_trigger_port': 50052,
                'confirmation_port': 50053,
                'confirmation_resp_port': 50012,
                'data_fetcher_port': 50010,
                'router_port': 50004,
                'status_check_port': 50050,
                'status_check_resp_port': 50011
            },
            'eventdetector': {
                'dirs_not_to_create': [],
                'event_det_port': 50003,
                'ext_data_port': 50101
            },
            'general': {
                'com_port': 50000,
                'control_pub_port': 50005,
                'control_sub_port': 50006,
                'ldapuri': 'it-ldap-slave.desy.de:1389',
                'log_size': 10485760,
                'request_fw_port': 50002,
                'request_port': 50001
            }
        }

        res = utils.map_conf_format(
            flat_config,
            config_type="sender",
            is_namespace=False
        )

        self.assertDictEqual(res, expected_result)

    def test_map_conf_format_full(self):

        flat_config = {
            'datafetcher': {
                'data_stream_targets': [['my_pc', 50100]],
                'file_fetcher': {
                    'fix_subdirs': self.fix_subdirs,
                },
                'http_fetcher': {
                    'fix_subdirs': self.fix_subdirs,
                },
                'local_target': '/my_dir/hidra/data/target',
                'number_of_streams': 1,
                'remove_data': True,
                'store_data': False,
                'type': 'file_fetcher',
                'use_data_stream': False
            },
            'eventdetector': {
                'http_events': {
                    'det_api_version': '1.6.0',
                    'det_ip': 'asap3-mon',
                    'fix_subdirs': self.fix_subdirs,
                    'history_size': 0
                },
                'inotifyx_events': {
                    'action_time': 150,
                    'create_fix_subdirs': False,
                    'fix_subdirs': self.fix_subdirs,
                    'history_size': 0,
                    'monitored_dir': '/my_dir/hidra/data/source',
                    'monitored_events': {'IN_MOVED_TO': ['.metadata']},
                    'time_till_closed': 2,
                    'use_cleanup': False
                },
                'type': 'watchdog_events',
                'watchdog_events': {
                    'action_time': 5,
                    'create_fix_subdirs': False,
                    'fix_subdirs': self.fix_subdirs,
                    'monitored_dir': '/my_dir/hidra/data/source',
                    'monitored_events': {'IN_MOVED_TO': ['.metadata']},
                    'time_till_closed': 1
                }
            },
            'general': {
                'ext_ip': '0.0.0.0',
                'log_name': 'datamanager.log',
                'log_path': '/my_dir/hidra/logs',
                'procname': 'hidra',
                'username': 'kuhnm',
                'whitelist': None
            }
        }

        expected_result = {
            'datafetcher': {
                'data_stream_targets': [['my_pc', 50100]],
                'file_fetcher': {
                    'fix_subdirs': self.fix_subdirs,
                },
                'http_fetcher': {
                    'fix_subdirs': self.fix_subdirs,
                },
                'local_target': '/my_dir/hidra/data/target',
                'number_of_streams': 1,
                'remove_data': True,
                'store_data': False,
                'type': 'file_fetcher',
                'use_data_stream': False
            },
            'eventdetector': {
                'http_events': {
                    'det_api_version': '1.6.0',
                    'det_ip': 'asap3-mon',
                    'fix_subdirs': self.fix_subdirs,
                    'history_size': 0
                },
                'inotifyx_events': {
                    'action_time': 150,
                    'create_fix_subdirs': False,
                    'fix_subdirs': self.fix_subdirs,
                    'history_size': 0,
                    'monitored_dir': '/my_dir/hidra/data/source',
                    'monitored_events': {'IN_MOVED_TO': ['.metadata']},
                    'time_till_closed': 2,
                    'use_cleanup': False
                },
                'type': 'watchdog_events',
                'watchdog_events': {
                    'action_time': 5,
                    'create_fix_subdirs': False,
                    'fix_subdirs': self.fix_subdirs,
                    'monitored_dir': '/my_dir/hidra/data/source',
                    'monitored_events': {'IN_MOVED_TO': ['.metadata']},
                    'time_till_closed': 1
                }
            },
            'general': {
                'ext_ip': '0.0.0.0',
                'log_name': 'datamanager.log',
                'log_path': '/my_dir/hidra/logs',
                'procname': 'hidra',
                'username': 'kuhnm',
                'whitelist': None
            }
        }

        res = utils.map_conf_format(
            flat_config,
            config_type="sender",
            is_namespace=False
        )

        self.assertDictEqual(res, expected_result)

    def test_map_conf_format_minimal(self):

        flat_config = {
            'config_file': '/my_dir/hidra/conf/datamanager_test.yaml',
            'onscreen': 'debug',
            'verbose': True
        }

        expected_result = {
            u'general': {
                u'config_file': '/my_dir/hidra/conf/datamanager_test.yaml',
                u'onscreen': 'debug',
                u'verbose': True
            }
        }

        res = utils.map_conf_format(
            flat_config,
            config_type="sender",
            is_namespace=False
        )

        self.assertDictEqual(res, expected_result)

    def tearDown(self):
        super().tearDown()
