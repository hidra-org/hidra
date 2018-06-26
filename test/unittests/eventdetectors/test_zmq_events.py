"""Testing the zmq_events event detector.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import copy
import json
import mock
import os
import time
import zmq

from .__init__ import BASE_DIR
from .eventdetector_test_base import EventDetectorTestBase
from test_base import create_dir, MockLogging, mock_get_logger
import zmq_events
import utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class MockZmqSocket(mock.MagicMock):

    def __init__(self, **kwds):
        super(MockZmqSocket, self).__init__(**kwds)
        self._connected = False

    def bind(self, endpoint):
        assert not self._connected
        assert endpoint != ""
        self._connected = True

    def connect(self, endpoint):
        assert not self._connected
        assert endpoint != ""
        self._connected = True

    def close(self, linger):
        assert self._connected
        self._connected = False


class MockZmqContext(mock.MagicMock):

    def __init__(self, **kwargs):
        super(MockZmqContext, self).__init__(**kwargs)
        self._destroyed = False
        self.IPV6 = None
        self.RCVTIMEO = None

    def socket(self, sock_type):
        assert not self._destroyed
#        assert self.IPV6 == 1
#        assert self.RCVTIMEO is not None
#        assert sock_type == zmq.REQ
        return MockZmqSocket()

    def destroy(self, linger):
        assert not self._destroyed
        self._destroyed = True


class TestEventDetector(EventDetectorTestBase):
    """Specification of tests to be performed for the loaded EventDetecor.
    """

    def setUp(self):
        super(TestEventDetector, self).setUp()

        # attributes inherited from parent class:
        # self.config
        # self.con_ip
        # self.ext_ip

        ipc_dir = self.config["ipc_dir"]
        create_dir(directory=ipc_dir, chmod=0o777)

        self.context = zmq.Context()

        self.eventdetector_config = {
            "context": self.context,
            "ipc_dir": ipc_dir,
            "con_ip": self.con_ip,
            "number_of_streams": 1,
            "ext_ip": self.ext_ip,
            "event_det_port": 50003,
            "main_pid": self.config["main_pid"]
        }

        self.start = 100
        self.stop = 101

        target_base_path = os.path.join(BASE_DIR, "data", "source")
        target_relative_path = os.path.join("local", "raw")
        self.target_path = os.path.join(target_base_path,
                                        target_relative_path)

        self.ipc_endpoints = zmq_events.get_ipc_endpoints(
            config=self.eventdetector_config
        )
        self.tcp_endpoints = zmq_events.get_tcp_endpoints(
            config=self.eventdetector_config
        )
        self.addrs = zmq_events.get_addrs(ipc_endpoints=self.ipc_endpoints,
                               tcp_endpoints=self.tcp_endpoints)

        self.eventdetector = None
        self.event_socket = None

    ######################################
    #             Test config            #
    ######################################

    @mock.patch("zmq_events.EventDetector.setup")
    def test_config_check(self, mock_setup):

        def check_params(eventdetector, ref_config):
            params_to_check = ref_config.keys()
            eventdetector.log.error = mock.Mock()

            for param in params_to_check:
                try:
                    eventdetector.config = copy.deepcopy(ref_config)
                    del eventdetector.config[param]

                    self.assertRaises(utils.WrongConfiguration,
                                      eventdetector.check_config)

                    # check that this is the only missing parameter
                    msg = ("Configuration of wrong format. Missing parameter: "
                           "'{}'".format(param))
                    eventdetector.log.error.assert_called_with(msg)
                    eventdetector.log.error.reset_mock()
                except AssertionError:
                    self.log.debug("checking param {}".format(param))
                    raise

        # test Linux
        with mock.patch.object(utils, "is_windows") as mock_is_windows:
            mock_is_windows.return_value = False

            with mock.patch("zmq_events.EventDetector.check_config"):
                eventdetector = zmq_events.EventDetector({}, self.log_queue)

            ref_config = {
                "context": None,
                "ipc_dir": None,
                "main_pid": None,
                "number_of_streams": None,
            }

            check_params(eventdetector, ref_config)

        # test Windows
        with mock.patch.object(utils, "is_windows") as mock_is_windows:
            mock_is_windows.return_value = True

            with mock.patch("zmq_events.EventDetector.check_config"):
                eventdetector = zmq_events.EventDetector({}, self.log_queue)

            ref_config = {
                "context": None,
                "number_of_streams": None,
                "ext_ip": None,
                # "con_ip": None,
                "event_det_port": None,
            }

            check_params(eventdetector, ref_config)

    ######################################
    #            Test helpers            #
    ######################################

    def test_get_tcp_endpoints(self):
        config = {
            "con_ip": self.con_ip,
            "ext_ip": self.ext_ip,
            "event_det_port": 50003,
        }

        # Linux
        with mock.patch.object(utils, "is_windows") as mock_is_windows:
            mock_is_windows.return_value = False

            endpoints = zmq_events.get_tcp_endpoints(config)
            self.assertIsNone(endpoints)

        # Windows
        with mock.patch.object(utils, "is_windows") as mock_is_windows:
            mock_is_windows.return_value = True

            endpoints = zmq_events.get_tcp_endpoints(config)
            port = config["event_det_port"]

            self.assertIsInstance(endpoints, zmq_events.TcpEndpoints)
            self.assertEqual(endpoints.eventdet_bind,
                             "{}:{}".format(self.ext_ip, port))
            self.assertEqual(endpoints.eventdet_con,
                             "{}:{}".format(self.con_ip, port))

    def test_get_ipc_endpoints(self):
        config = {
            "ipc_dir": self.config["ipc_dir"],
            "main_pid": self.config["main_pid"],
        }

        # Linux
        with mock.patch.object(utils, "is_windows") as mock_is_windows:
            mock_is_windows.return_value = False

            endpoints = zmq_events.get_ipc_endpoints(config)
            main_pid = config["main_pid"]

            self.assertIsInstance(endpoints, zmq_events.IpcEndpoints)
            self.assertEqual(endpoints.eventdet,
                             "/tmp/hidra/{}_eventDet".format(main_pid))

        # Windows
        with mock.patch.object(utils, "is_windows") as mock_is_windows:
            mock_is_windows.return_value = True

            endpoints = zmq_events.get_ipc_endpoints(config)
            self.assertIsNone(endpoints)

    def test_get_addrs(self):
        tcp_endpoints = zmq_events.TcpEndpoints(
            eventdet_bind="my_eventdet_bind",
            eventdet_con="my_eventdet_con",
        )
        ipc_endpoints = zmq_events.IpcEndpoints(eventdet="my_eventdet")

        # Linux
        addrs = zmq_events.get_addrs(ipc_endpoints=ipc_endpoints,
                                     tcp_endpoints=None)

        self.assertIsInstance(addrs, zmq_events.Addresses)
        self.assertEqual(addrs.eventdet_bind,
                         "ipc://{}".format("my_eventdet"))
        self.assertEqual(addrs.eventdet_con,
                         "ipc://{}".format("my_eventdet"))

        # Windows
        addrs = zmq_events.get_addrs(ipc_endpoints=None,
                                     tcp_endpoints=tcp_endpoints)

        self.assertIsInstance(addrs, zmq_events.Addresses)
        self.assertEqual(addrs.eventdet_bind,
                         "tcp://{}".format("my_eventdet_bind"))
        self.assertEqual(addrs.eventdet_con,
                         "tcp://{}".format("my_eventdet_con"))

    ######################################
    #             Test setup             #
    ######################################

    def todo_test_setup(self):

        with mock.patch("zmq_events.EventDetector.check_config"):
            with mock.patch("zmq_events.EventDetector.setup"):
                evtdet = EventDetector({}, self.log_queue)

        evtdet.config = {
            "context": MockZmqContext(),
            "ipc_dir": self.config["ipc_dir"],
            "con_ip": self.con_ip,
            "ext_ip": self.ext_ip,
            "event_det_port": 50003,
            "main_pid": self.config["main_pid"]
        }

        # platform independent
#        @mock.patch.object(utils, "get_logger", mock_get_logger)
        with mock.patch("zmq.Context"):
            evtdet.setup()

#        self.assertTrue(evtdet.ext_context)

        evtdet.stop()

    ######################################
    #              Test run              #
    ######################################

    def test_eventdetector(self):
        """Simulate incoming data and check if received events are correct.
        """

#        with mock.patch.object(Parent, 'test_method') as mock_method:
#        with mock.patch("utils.get_logger"):
#        with mock.patch.object(utils, "get_logger", mock_get_logger):
#            with mock.patch("zmq_events.EventDetector.setup"):
#                self.eventdetector = EventDetector(self.eventdetector_config,
#                                                   self.log_queue)
#
#        with mock.patch.object(zmq, "Context", MockZmqContext):
#            self.eventdetector.setup()

        self.eventdetector = zmq_events.EventDetector(self.eventdetector_config,
                                                      self.log_queue)

        # create zmq socket to send events
        try:
            self.event_socket = self.context.socket(zmq.PUSH)
            self.event_socket.connect(self.addrs.eventdet_con)
            self.log.info("Start event_socket (connect): '{}'"
                          .format(self.addrs.eventdet_con))
        except:
            self.log.error("Failed to start event_socket (connect): '{}'"
                           .format(self.addrs.eventdet_con))
            raise

        for i in range(self.start, self.stop):
            try:
                self.log.debug("generate event")
                target_file = "{}{}.cbf".format(self.target_path, i)
                message = {
                    u"filename": target_file,
                    u"filepart": 0,
                    u"chunksize": 10
                }

                self.event_socket.send_multipart(
                    [json.dumps(message).encode("utf-8")]
                )

                event_list = self.eventdetector.get_new_event()
                if event_list:
                    self.log.debug("event_list: {}".format(event_list))

#                self.assertEqual(len(event_list), 1)
#                self.assertDictEqual(event_list[0], expected_result_dict)
                self.assertIn(message, event_list)

                time.sleep(1)
            except KeyboardInterrupt:
                break

        message = [b"CLOSE_FILE", "test_file.cbf".encode("utf8")]
        self.event_socket.send_multipart(message)

        event_list = self.eventdetector.get_new_event()
        self.log.debug("event_list: {}".format(event_list))

        self.assertIn(message, event_list)

    def tearDown(self):
        if self.event_socket is not None:
            self.event_socket.close(0)
            self.event_socket = None

        if self.eventdetector is not None:
            self.eventdetector.stop()
            self.eventdetector = None
        self.context.destroy(0)

        super(TestEventDetector, self).tearDown()
