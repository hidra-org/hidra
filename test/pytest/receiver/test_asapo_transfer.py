from pathlib import Path
import sys
import pytest
from os import path
import logging
from time import time
from unittest.mock import create_autospec, patch, call
import threading
import time
from threading import Event

receiver_path = (Path(__file__).parent.parent.parent.parent / "src/hidra/receiver")
assert receiver_path.is_dir()
sys.path.insert(0, str(receiver_path))

from plugins.asapo_producer import AsapoWorker
from asapo_transfer import run_transfer, TransferConfig, verify_config, construct_config


logger = logging.getLogger(__name__)


def wait_for(cond, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        if cond():
            return True
        time.sleep(0.1)

    return cond()


@pytest.fixture
def worker_config():
    config = dict(
        endpoint="asapo-services:8400",
        beamline="p00",
        beamtime='auto',
        token="abcdefg1234=",
        default_data_source='test001',
        n_threads=1,
        file_regex=".*raw/(?P<scan_id>.*)_(?P<file_idx_in_scan>.*).h5",
    )
    return config


@pytest.fixture
def worker(worker_config):
    worker_config = worker_config.copy()
    worker = AsapoWorker(**worker_config)
    worker.send_message = create_autospec(worker.send_message)
    yield worker


@pytest.fixture
def transfer_config(worker_config):
    init_config = dict(
        detector_id="foo",
        signal_host="localhost",
        target_host="localhost",
        target_dir='',
        receive_timeout=1,
    )
    init_config.update(worker_config)
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
def mock_transfer(hidra_metadata):
    with patch('asapo_transfer.Transfer', autospec=True) as mock_transfer:
        query = mock_transfer.return_value
        def get_return():
            for metadata in hidra_metadata:
                yield metadata, None
            # return the same metadata forever, but slower
            metadata = {
                'source_path': 'http://127.0.0.1/data', 'relative_path': "/ramdisk",
                'filename': "raw/foo.txt", 'version': '4.4.2', 'chunksize': 10485760,
                'file_mod_time': 1643637926.004875,
                'file_create_time': 1643637926.0048773, 'confirmation_required': False,
                'chunk_number': 0}
            while True:
                time.sleep(0.1)
                yield metadata, None

        query.get.side_effect = get_return()
        yield mock_transfer


def test_verify_config():
    verify_config({"endpoint": "localhost:8000", "send_timeout": 10, "log_level": "INFO"})


def test_verify_config_missing_endpoint():
    with pytest.raises(ValueError):
        verify_config({"timeout": 10})


def test_verify_config_wrong_type():
    with pytest.raises(ValueError):
        verify_config({"endpoint": "localhost:8000", "send_timeout": "10"})


def test_verify_config_unexpected__parameter():
    with pytest.raises(ValueError):
        verify_config({"endpoint": "localhost:8000", "send_timeout": 10, "beamline": "p00"})


def test_verify_config_wrong_log_level():
    with pytest.raises(ValueError):
        verify_config({"endpoint": "localhost:8000", "send_timeout": 10, "log_level": "FOO"})


def test_construct_config(tmp_path):
    config_file = tmp_path / "asapo_transfer_p00.yaml"
    config_file.write_text(
    f"""
endpoint: localhost:8000
token_path: {tmp_path}
"""
    )
    token_file = tmp_path / "p00.token"
    token_file.write_text("abcdefg1234=")

    config = construct_config(str(tmp_path), "p00_det100.desy.de")

    assert config.endpoint == "localhost:8000"
    assert config.token == "abcdefg1234="
    assert config.beamline == "p00"
    assert config.detector_id == "det100.desy.de"
    assert config.default_data_source == "det100"
    assert config.signal_host == "asap3-p00"
    assert config.target_host


def test_run_transfer(worker, transfer_config, file_list, hidra_metadata, mock_transfer):
    event = Event()
    t = threading.Thread(target=run_transfer, args=(worker, transfer_config, 0.1, event))
    t.start()

    def cond():
        # wait for the last file to be send
        return call(file_list[-1], hidra_metadata[-1]) in worker.send_message.call_args_list

    wait_for(cond)

    event.set()
    t.join()

    expected_call_args = [
        call(filename, metadata) for filename, metadata in zip(file_list, hidra_metadata)]
    worker.send_message.call_args_list ==  expected_call_args
    query = mock_transfer.return_value
    assert query.stop.call_count == mock_transfer.call_count


def test_run_transfer_query_start_fails(worker, transfer_config, mock_transfer):
    query = mock_transfer.return_value
    query.start.side_effect = [Exception, None]
    event = Event()
    t = threading.Thread(target=run_transfer, args=(worker, transfer_config, 0.1, event))
    t.start()

    def cond():
        # query error results in fresh query object creation
        return mock_transfer.call_count >= 2

    wait_for(cond)

    event.set()
    t.join()

    # assert condition only after join
    assert cond()
    assert query.stop.call_count == mock_transfer.call_count


def test_run_transfer_query_initiate_fails(worker, transfer_config, mock_transfer):
    query = mock_transfer.return_value
    query.initiate.side_effect = [Exception, None]
    event = Event()
    t = threading.Thread(target=run_transfer, args=(worker, transfer_config, 0.1, event))
    t.start()

    def cond():
        # query error results in fresh query object creation
        return mock_transfer.call_count >= 2

    wait_for(cond)

    event.set()
    t.join()

    # assert condition only after join
    assert cond()
    assert query.stop.call_count == mock_transfer.call_count


def test_run_transfer_query_get_fails(worker, transfer_config, file_list, hidra_metadata, mock_transfer):
    query = mock_transfer.return_value
    query.get.side_effect = [(hidra_metadata[0], None), Exception, (hidra_metadata[1], None)]
    event = Event()
    t = threading.Thread(target=run_transfer, args=(worker, transfer_config, 0.1, event))
    t.start()

    def cond():
        # wait until all get return value were sent
        return worker.send_message.call_count == 2

    wait_for(cond)

    event.set()
    t.join()

    assert mock_transfer.call_count >= 2
    worker.send_message.assert_any_call(file_list[0], hidra_metadata[0])
    worker.send_message.assert_any_call(file_list[1], hidra_metadata[1])
    assert query.stop.call_count == mock_transfer.call_count


def test_run_transfer_query_stop_fails(worker, transfer_config, file_list, hidra_metadata, mock_transfer):
    query = mock_transfer.return_value
    query.start.side_effect = [Exception] + [None]*1000
    query.stop.side_effect = [Exception] + [None]*1000
    event = Event()
    t = threading.Thread(target=run_transfer, args=(worker, transfer_config, 0.1, event))
    t.start()

    def cond():
        # wait until something was sent
        return worker.send_message.call_count > 0

    wait_for(cond)

    event.set()
    t.join()

    assert mock_transfer.call_count >= 2
    worker.send_message.assert_any_call(file_list[0], hidra_metadata[0])
    assert query.stop.call_count == mock_transfer.call_count


def test_run_transfer_asapo_send_fails(worker, transfer_config, file_list, hidra_metadata, mock_transfer, caplog):
    # the third call to send_message returns an error, more than 1000 calls should not happen
    worker.send_message.side_effect = [None, None, Exception] + [None]*1000
    event = Event()
    t = threading.Thread(target=run_transfer, args=(worker, transfer_config, 0.1, event))
    t.start()

    def cond():
        return worker.send_message.call_count >= 4

    wait_for(cond)

    event.set()
    t.join()

    assert mock_transfer.call_count == 1  # send error doesn't cause restart
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelname == "ERROR"
    assert "Transmission does not succeed" in record.message
