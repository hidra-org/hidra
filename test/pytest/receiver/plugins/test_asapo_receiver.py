from pathlib import Path
import sys
import pytest
from os import path
from time import time
from unittest.mock import create_autospec
import asapo_producer

receiver_path = (
        Path(__file__).parent.parent.parent.parent.parent / "src/hidra/receiver")
assert receiver_path.is_dir()
sys.path.insert(0, receiver_path)

from plugins.asapo_producer import Plugin, AsapoWorker  # noqa


@pytest.fixture
def config():
    config = dict(
        endpoint="asapo-services:8400",
        beamtime="p00",
        token="abcdefg1234=",
        data_source='test001',
        n_threads=1,
        file_regex=".*/(?P<detector>.*)/(?P<scan_id>.*)_scan[0-9]*-(?P<file_idx_in_scan>.*).tif",
        user_config_path="Path"
    )
    return config


@pytest.fixture
def worker(config):
    config = {k: v for k, v in config.items() if k != "user_config_path"}
    worker = AsapoWorker(**config)
    worker.send_message = create_autospec(worker.send_message)
    yield worker


@pytest.fixture
def plugin(config):
    plugin = Plugin(config)
    yield plugin


@pytest.fixture
def filepath():
    return "/tmp/hidra_source/current/raw/det01/stream100_scan0-107.tif"


@pytest.fixture
def metadata(filepath):
    return {'relative_path': 'current/stream100_scan0-107.tif',
            'filename': 'stream100_scan0-107.tif'}


def test_worker(worker, metadata, filepath):
    assert worker.ingest_mode == asapo_producer.INGEST_MODE_TRANSFER_METADATA_ONLY
    data_source, stream, file_idx = worker._parse_file_name(filepath)
    assert file_idx == 107
    assert stream == 'stream100'
    assert data_source == 'test001'


def test_config_time(plugin, metadata):
    plugin.setup()

    try:
        plugin.config_timeout = 1
        plugin._get_config_time("bla")
    except FileNotFoundError as err:
        print("ERR: ", err)
        assert "No such file or directory" in str(err)

    file_path = Path(__file__)
    conf_time = path.getmtime(file_path)
    assert conf_time == plugin._get_config_time(file_path)


def test_config_modified(plugin):
    plugin._get_config_time = create_autospec(plugin._get_config_time, return_value=100)

    plugin.config_time = 99
    plugin.check_time = 0
    assert plugin._config_is_modified()

    plugin.config_time = 100
    plugin.check_time = 0
    assert not plugin._config_is_modified()

    plugin.config_time = 99
    plugin.check_time = time()
    assert not plugin._config_is_modified()
