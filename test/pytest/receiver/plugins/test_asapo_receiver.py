from pathlib import Path
import sys
import pytest
from unittest.mock import create_autospec
import asapo_producer

receiver_path = (
    Path(__file__).parent.parent.parent.parent.parent / "src/hidra/receiver")
assert receiver_path.is_dir()
sys.path.insert(0, receiver_path)

from plugins.asapo_producer import Plugin  # noqa


@pytest.fixture
def plugin():
    plugin = Plugin(dict(
        endpoint="asapo-services:8400",
        beamline="p00",
        beamtime="auto",
        token="abcdefg1234=",
        data_source='test001',
        n_threads=1,
        ingest_mode="INGEST_MODE_TRANSFER_METADATA_ONLY",
        file_regex=".*/(?P<detector>.*)/(?P<scan_id>.*)_scan[0-9]*-(?P<file_idx_in_scan>.*).tif",
        ignore_regex=".*/.*.metadata$",
    ))
    plugin.send_message = create_autospec(plugin.send_message)
    plugin._get_file_idx = create_autospec(plugin._get_file_idx, return_value=1)
    yield plugin


@pytest.fixture
def plugin_sequential():
    plugin = Plugin(dict(
        endpoint="asapo-services:8400",
        beamline="p00",
        beamtime="auto",
        token="abcdefg1234=",
        data_source='test001',
        n_threads=1,
        ingest_mode="DEFAULT_INGEST_MODE",
        file_regex=".*/(?P<detector>.*)/(?P<scan_id>.*)_scan[0-9]*-(?P<file_idx_in_scan>.*).tif",
        ignore_regex=".*/.*.metadata$",
        sequential_idx=True
    ))
    plugin.send_message = create_autospec(plugin.send_message)
    plugin._get_file_idx = create_autospec(plugin._get_file_idx, return_value=1)
    yield plugin


@pytest.fixture
def filepath():
    return "/tmp/hidra_source/current/raw/det01/stream100_scan0-107.tif"


@pytest.fixture
def metadata(filepath):
    return {'relative_path': 'current/stream100_scan0-107.tif',
            'filename': 'stream100_scan0-107.tif'}


def test_setup(plugin, metadata, filepath):
    plugin.setup()
    assert plugin.ingest_mode == asapo_producer.INGEST_MODE_TRANSFER_METADATA_ONLY

    plugin.process(local_path=filepath, metadata=metadata)
    _, local_path, file_idx, stream, send_meta = plugin.send_message.call_args[0]
    assert local_path == filepath
    assert file_idx == 107
    assert stream == 'stream100'
    assert send_meta == metadata


def test_sequential(plugin_sequential, metadata, filepath):
    plugin_sequential.setup()
    assert plugin_sequential.ingest_mode == asapo_producer.DEFAULT_INGEST_MODE

    plugin_sequential.process(local_path=filepath, metadata=metadata)
    _, local_path, file_idx, stream, send_meta = plugin_sequential.send_message.call_args[0]
    assert local_path == filepath
    assert file_idx == 1
    assert stream == 'stream100'
    assert send_meta == metadata
