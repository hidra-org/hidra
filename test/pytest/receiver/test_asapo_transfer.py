from pathlib import Path
import sys
import pytest
from os import path
import logging
from time import time, sleep
from unittest.mock import create_autospec, patch, Mock
import asapo_producer
import threading
import zmq
from threading import Event

receiver_path = (Path(__file__).parent.parent.parent.parent / "src/hidra/receiver")
assert receiver_path.is_dir()
sys.path.insert(0, str(receiver_path))

from plugins.asapo_producer import Plugin, AsapoWorker
import asapo_transfer
from asapo_transfer import AsapoTransfer, run_transfer, TransferConfig, create_query, varify_config
from hidra import Transfer


logger = logging.getLogger(__name__)


@pytest.fixture
def config():
    config = dict(
        endpoint="asapo-services:8400",
        beamline="p00",
        beamtime='auto',
        token="abcdefg1234=",
        default_data_source='test001',
        n_threads=1,
        file_regex=".*raw/(?P<scan_id>.*)_(?P<file_idx_in_scan>.*).h5",
        user_config_path="/path/to/config.yaml"
    )
    return config


@pytest.fixture
def transfer_config():
    transfer_config = dict(
        detector_id="foo",
        signal_host="localhost",
        target_host="localhost",
        target_port="50101",
        target_dir='',
        reconnect_timeout=1,
    )
    return transfer_config


@pytest.fixture
@patch('asapo_transfer.read_token', create_autospec(asapo_transfer.read_token))
def transfer_config_obj(transfer_config, config):
    init_config = transfer_config.copy()
    init_config.pop("target_port")
    init_config['endpoint'] = config['endpoint']
    init_config['beamline'] = config['beamline']
    init_config['token'] = config['token']
    init_config['default_data_source'] = config['default_data_source']
    return TransferConfig(**init_config)


@pytest.fixture
def file_list():
    file_list = ['current/raw/foo1/bar1_1.h5', 'current/raw/foo1/bar2_1.h5',
                 'current/raw/foo2/bar1_1.h5', 'current/raw/foo2/bar2_1.h5',
                 'local/raw/foo1/bar1_1.h5', 'local/raw/foo1/bar2_1.h5',
                 'local/raw/foo2/bar1_1.h5', 'local/raw/foo2/bar2_1.h5',
                 'commissioning/raw/foo1/bar1_1.h5', 'commissioning/raw/foo1/bar2_1.h5',
                 'commissioning/raw/foo2/bar1_1.h5', 'commissioning/raw/foo2/bar2_1.h5',
                 ]
    return file_list


@pytest.fixture
def hidra_metadata(file_list):
    hidra_metadata = []
    for file_path in file_list:
        hidra_metadata.append({'source_path': 'http://127.0.0.1/data', 'relative_path': path.dirname(file_path),
                               'filename': path.basename(file_path), 'version': '4.4.2', 'chunksize': 10485760,
                               'file_mod_time': 1643637926.004875,
                               'file_create_time': 1643637926.0048773, 'confirmation_required': False,
                               'chunk_number': 0})
    return hidra_metadata


@pytest.fixture
def worker(config):
    worker_config = config.copy()
    del worker_config["user_config_path"]
    worker = AsapoWorker(**worker_config)
    worker.send_message = create_autospec(worker.send_message)
    yield worker


@pytest.fixture
def query(config):
    query = create_autospec(Transfer, instance=True)
    yield query


@pytest.fixture
def asapo_transfer(worker, query, transfer_config, hidra_metadata):
    asapo_transfer = AsapoTransfer(worker, query,
                                   transfer_config['target_host'],
                                   transfer_config['target_port'],
                                   transfer_config['target_dir'], transfer_config['reconnect_timeout'])
    return_list = [[metadata, None] for metadata in hidra_metadata]
    asapo_transfer.query.get.side_effect = return_list
    yield asapo_transfer


def test_varify_config(config):
    with pytest.raises(ValueError):
        varify_config(config)
    del config["user_config_path"]
    del config["beamline"]
    del config["token"]
    varify_config(config)


def test_asapo_transfer(file_list, asapo_transfer, hidra_metadata):

    x = threading.Thread(target=asapo_transfer.run, args=())
    x.start()
    sleep(1)
    asapo_transfer.stop()
    sleep(1)
    x.join()
    asapo_transfer.asapo_worker.send_message.assert_called_with(file_list[-1], hidra_metadata[-1])
    asapo_transfer.query.stop.assert_called_with()


def create_mock_transfer(asapo_worker, *kwargs):
    asapo_transfer = Mock()
    asapo_transfer.run = Mock(return_value=True)
    asapo_worker.send_message(None, None)
    return asapo_transfer


def create_mock_failing_transfer(asapo_worker, *kwargs):
    asapo_transfer = Mock()
    asapo_transfer.run = Mock()
    asapo_transfer.run.side_effect = Exception('mocked error')
    asapo_worker.send_message(None, None)
    return asapo_transfer


@patch('asapo_transfer.create_query', create_autospec(create_query, instance=True))
@patch('asapo_transfer.create_asapo_transfer', create_mock_transfer)
def test_run_transfer(worker, transfer_config_obj, file_list, hidra_metadata):

    stop_event = Event()
    x = threading.Thread(target=run_transfer, args=(worker, transfer_config_obj, 1, stop_event))
    x.start()
    sleep(2)
    stop_event.set()
    sleep(1)
    x.join()
    worker.send_message.assert_called_with(None, None)


@patch('asapo_transfer.create_query', create_autospec(create_query, instance=True))
@patch('asapo_transfer.create_asapo_transfer', create_mock_failing_transfer)
def test_stop_error(worker, transfer_config_obj, file_list, hidra_metadata):

    stop_event = Event()
    x = threading.Thread(target=run_transfer, args=(worker, transfer_config_obj, 1, stop_event))
    x.start()
    sleep(5)
    stop_event.set()
    sleep(1)
    x.join()
    worker.send_message.assert_called_with(None, None)
    # asapo_transfer is crashed and restarted several times
    assert(len(worker.send_message.mock_calls) > 3)


def test_path_parsing(asapo_transfer, file_list, hidra_metadata):
    asapo_transfer.stop_run.is_set = Mock()
    asapo_transfer.stop_run.is_set.side_effect = [False, False, True]
    asapo_transfer.run()

    asapo_transfer.asapo_worker.send_message.assert_called_with(file_list[0], hidra_metadata[0])

    stream_list = ['foo1/bar1', 'foo1/bar2', 'foo2/bar1', 'foo2/bar2']*3
    # ToDo: Refactor: Extract free function or so
    for i, file_path in enumerate(file_list):
        data_source, stream, file_idx = asapo_transfer.asapo_worker._parse_file_name(file_path)
        assert file_idx == 1
        assert stream == stream_list[i]
        assert data_source == 'test001'


def test_path_parse_no_offset(asapo_transfer):
    # Offset is not encoded in the file path
    file_path = "current/raw/data/tilt_27_048_041_data_070421.h5"
    data_source, stream, file_idx = asapo_transfer.asapo_worker._parse_file_name(file_path)
    assert file_idx == 70421


def test_path_parse_nonzero_offset(asapo_transfer):
    ragexp_template = "current/raw/(?P<scan_id>.*)_(?P<file_idx_offset>.*)_(?P<file_idx_in_scan>.*).h5"
    asapo_transfer.asapo_worker.file_regex = ragexp_template

    # Offset is encoded in the file path
    file_path = "current/raw/data/tilt_27_048_041_data_070421_000001.h5"
    data_source, stream, file_idx = asapo_transfer.asapo_worker._parse_file_name(file_path)
    assert file_idx == 70422


def test_path_parce_failed(asapo_transfer):
    ragexp_template = "current/raw/(?P<scan_id>.*)_(?P<file_idx_offset>.*)_(?P<file_idx_in_scan>.*).h5"
    asapo_transfer.asapo_worker.file_regex = ragexp_template
    with pytest.raises(ValueError):
        file_path = "current/raw/data/tilt_27_048_041_data_070421_foo.h5"
        data_source, stream, file_idx = asapo_transfer.asapo_worker._parse_file_name(file_path)


def test_init_query(asapo_transfer, file_list, hidra_metadata):

    def initiate_effect():
        counter = 0
        while True:
            sleep(0.5)
            counter += 1
            if counter == 1:
                yield KeyError("foo")
            yield counter

    asapo_transfer.query.initiate.side_effect = initiate_effect()
    asapo_transfer.stop_run.is_set = Mock()
    asapo_transfer.stop_run.is_set.side_effect = [False, False, False, True]

    with pytest.raises(KeyError):
        asapo_transfer.run()

    asapo_transfer.query.start.assert_called_with([asapo_transfer.target_host, asapo_transfer.target_port])
    asapo_transfer.query.initiate.assert_called_with([[asapo_transfer.target_host, asapo_transfer.target_port, 1]])
    asapo_transfer.query.stop.assert_called_with()

    asapo_transfer.run()
    asapo_transfer.query.start.assert_called_with([asapo_transfer.target_host, asapo_transfer.target_port])
    asapo_transfer.query.initiate.assert_called_with([[asapo_transfer.target_host, asapo_transfer.target_port, 1]])
    asapo_transfer.asapo_worker.send_message.assert_called_with(file_list[0], hidra_metadata[0])
    asapo_transfer.query.stop.assert_called_with()


def test_start_query(asapo_transfer, file_list, hidra_metadata):
    def start_effect():
        counter = 0
        while True:
            sleep(0.5)
            counter += 1
            if counter == 1:
                yield KeyError("foo")
            yield counter

    asapo_transfer.query.start.side_effect = start_effect()

    asapo_transfer.stop_run.is_set = Mock()
    asapo_transfer.stop_run.is_set.side_effect = [False, False, False, True]

    with pytest.raises(KeyError):
        asapo_transfer.run()

    asapo_transfer.query.start.assert_called_with([asapo_transfer.target_host, asapo_transfer.target_port])
    asapo_transfer.query.stop.assert_called_with()

    asapo_transfer.run()
    asapo_transfer.query.start.assert_called_with([asapo_transfer.target_host, asapo_transfer.target_port])
    asapo_transfer.query.initiate.assert_called_with([[asapo_transfer.target_host, asapo_transfer.target_port, 1]])
    asapo_transfer.asapo_worker.send_message.assert_called_with(file_list[0], hidra_metadata[0])
    asapo_transfer.query.stop.assert_called_with()
