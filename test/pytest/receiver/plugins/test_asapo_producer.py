from pathlib import Path
import json
import pytest
from os import path
from time import time
from unittest.mock import create_autospec, patch
import asapo_producer

from plugins.asapo_producer import Plugin, AsapoWorker  # noqa


@pytest.fixture
def config():
    config = dict(
        endpoint="asapo-services:8400",
        beamtime="p00",
        token="abcdefg1234=",
        default_data_source='test001',
        n_threads=1,
        file_regex=(
            ".*/(?P<detector>.*)/(?P<scan_id>.*)"
            "_scan[0-9]*-(?P<file_idx_in_scan>.*).tif"
        ),
        user_config_path="/path/to/config.yaml"
    )
    return config


@pytest.fixture
def mock_create_producer(monkeypatch):
    mock = create_autospec(asapo_producer.create_producer)
    monkeypatch.setattr(asapo_producer, "create_producer", mock)
    return mock


@pytest.fixture
def mock_producer(mock_create_producer):
    return mock_create_producer.return_value


@pytest.fixture
def worker(config):
    worker_config = config.copy()
    del worker_config["user_config_path"]
    worker = AsapoWorker(**worker_config)
    return worker


@pytest.fixture
def plugin(config):
    plugin = Plugin(config)
    yield plugin


def test_worker_create_producer(worker, mock_create_producer, config):
    filepath = "/tmp/hidra_source/current/raw/det01/stream100_scan0-107.tif"
    metadata = {
        "relative_path": "current/raw/det01",
        "filename": "stream100_scan0-107.tif"
    }
    worker.send_message(filepath, metadata)

    mock_create_producer.assert_called_once_with(
        config["endpoint"],
        'raw',
        config["beamtime"],
        config.get("beamline", "auto"),
        config["default_data_source"],
        config["token"],
        config["n_threads"],
        config.get("timeout", 5) * 1000,
    )


def test_worker_send_message(worker, mock_producer):
    filepath = "/tmp/hidra_source/current/raw/det01/stream100_scan0-107.tif"
    metadata = {
        "relative_path": "current/raw/det01",
        "filename": "stream100_scan0-107.tif"
    }
    worker.send_message(filepath, metadata)

    mock_producer.send.assert_called_once()
    args, kwargs = mock_producer.send.call_args
    assert args == ()
    assert kwargs["id"] == 107
    assert kwargs["exposed_path"] == "raw/det01/stream100_scan0-107.tif"
    assert kwargs["data"] is None
    assert kwargs["ingest_mode"] == (
        asapo_producer.INGEST_MODE_TRANSFER_METADATA_ONLY
    )
    assert kwargs["stream"] == "stream100"
    assert kwargs["callback"] is not None
    user_meta = json.loads(kwargs["user_meta"])
    assert user_meta["hidra"] == metadata


def test_worker_non_matching_file(worker, mock_create_producer):
    filepath = "/tmp/hidra_source/current/raw/det01/scan0-107.tif"
    metadata = {
        "relative_path": "current/raw/det01",
        "filename": "scan0-107.tif"
    }
    worker.send_message(filepath, metadata)

    mock_create_producer.assert_not_called()


def test_worker_file_index_match_not_an_integer(worker, mock_create_producer, caplog):
    filepath = "/tmp/hidra_source/current/raw/det01/stream100_scan0-foo.tif"
    metadata = {
        "relative_path": "current/raw/det01",
        "filename": "stream100_scan0-foo.tif"
    }
    worker.send_message(filepath, metadata)

    mock_create_producer.assert_not_called()
    record = caplog.records[0]
    assert record.levelname == "WARNING"
    assert "Ignor" in record.message


def test_worker_stop(worker, mock_producer):
    filepath = "/tmp/hidra_source/current/raw/det01/stream100_scan0-107.tif"
    metadata = {
        "relative_path": "current/raw/det01",
        "filename": "stream100_scan0-107.tif"
    }
    worker.send_message(filepath, metadata)
    worker.stop()
    mock_producer.wait_requests_finished.assert_called_once()


def test_worker_stop_no_producer(worker):
    worker.stop()


def test_worker_data_source_regex(config, mock_create_producer):
    del config["user_config_path"]
    config["file_regex"] = (
        ".*/(?P<data_source>.*)/(?P<scan_id>.*)"
        "_scan[0-9]*-(?P<file_idx_in_scan>.*).tif"
    )
    worker = AsapoWorker(**config)

    filepath = "/tmp/hidra_source/current/raw/det01/stream100_scan0-107.tif"
    metadata = {
        "relative_path": "current/raw/det01",
        "filename": "stream100_scan0-107.tif"
    }
    worker.send_message(filepath, metadata)

    assert "det01" in mock_create_producer.call_args.args


def test_worker_index_offset_regex(config, mock_producer):
    del config["user_config_path"]
    config["file_regex"] = (
        ".*/(?P<detector>.*)/(?P<scan_id>.*)"
        "_scan[0-9]*_(?P<file_idx_offset>[0-9]+)-(?P<file_idx_in_scan>.*).tif"
    )
    worker = AsapoWorker(**config)

    filepath = (
        "/tmp/hidra_source/current/raw/det01/stream100_scan0_123-107.tif"
    )
    metadata = {
        "relative_path": "current/raw/det01",
        "filename": "stream100_scan0_123-107.tif"
    }
    worker.send_message(filepath, metadata)

    assert mock_producer.send.call_args.kwargs["id"] == 230


def test_config_time(plugin):
    plugin.setup()
    assert plugin._get_config_time("bla") == 0

    file_path = Path(__file__)
    conf_time = path.getmtime(str(file_path))
    assert conf_time == plugin._get_config_time(str(file_path))


def test_config_modified(plugin):
    plugin._get_config_time = create_autospec(
        plugin._get_config_time, return_value=100)

    plugin.check_time = 0
    assert plugin._config_is_modified()
    assert not plugin._config_is_modified()

    plugin.check_time = time()
    assert not plugin._config_is_modified()


def test_plugin_stop(plugin):
    with patch("plugins.asapo_producer.AsapoWorker", autospec=True):
        plugin.process(None, None, None)  # will create a worker
        assert plugin.asapo_worker is not None
        plugin.stop()
        plugin.asapo_worker.stop.assert_called_once_with()


def test_plugin_stop_without_worker(plugin):
    assert plugin.asapo_worker is None
    plugin.stop()


def test_worker_data_source_removal(config, mock_create_producer):
    del config["user_config_path"]
    config["file_regex"] = (
        ".*/(?P<data_source>.*)/(?P<scan_id>.*)"
        "_scan[0-9]*-(?P<file_idx_in_scan>.*).tif"
    )
    worker = AsapoWorker(**config)

    # Create 11 data sources/producers
    for i in range(worker.max_active_data_sources + 1):
        filepath = "/tmp/hidra_source/current/raw/det{:02d}/stream100_scan0-107.tif".format(i)
        metadata = {
            "relative_path": "current/raw/det{:02d}".format(i),
            "filename": "stream100_scan0-107.tif"
        }
        worker.send_message(filepath, metadata)
        assert "det{:02d}".format(i) in mock_create_producer.call_args.args

    assert len(worker.data_source_info) == worker.max_active_data_sources
    assert "det00" not in worker.data_source_info
    assert next(iter(worker.data_source_info)) == "det01"

    # Send another message to move data source det01 to the end
    filepath = (
        "/tmp/hidra_source/current/raw/det01/stream100_scan0-108.tif"
    )
    metadata = {
        "relative_path": "current/raw/det01",
        "filename": "stream100_scan0-108.tif"
    }
    worker.send_message(filepath, metadata)

    assert next(iter(worker.data_source_info)) == "det02"

    # Create a new data source
    filepath = (
        "/tmp/hidra_source/current/raw/det99/stream100_scan0-107.tif"
    )
    metadata = {
        "relative_path": "current/raw/det99",
        "filename": "stream100_scan0-107.tif"
    }
    worker.send_message(filepath, metadata)

    assert "det99" in mock_create_producer.call_args.args
    assert len(worker.data_source_info) == worker.max_active_data_sources
    assert "det00" not in worker.data_source_info
    assert "det02" not in worker.data_source_info
    assert "det01" in worker.data_source_info
    assert "det99" in worker.data_source_info
    assert next(iter(worker.data_source_info)) == "det03"
