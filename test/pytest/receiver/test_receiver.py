import logging
from pathlib import Path
import sys
import time
import pytest

receiver_path = (
    Path(__file__).parent.parent.parent.parent / "src/hidra/receiver")
assert receiver_path.is_dir()
sys.path.insert(0, receiver_path)

from datareceiver import PluginHandler, run_plugin_thread  # noqa
import plugins.mock_plugin as mock_plugin_m  # noqa

log = logging.getLogger(__name__)

metadata = {
    "relative_path": "relative_path",
    "filename": "filename"}


@pytest.fixture
def mock_plugin():
    mock_plugin_m.Plugin.side_effect = None
    mock_plugin = mock_plugin_m.Plugin(None)
    mock_plugin_m.Plugin.reset_mock()
    mock_plugin.reset_mock()
    mock_plugin.setup.side_effect = None
    mock_plugin.process.side_effect = None
    mock_plugin.stop.side_effect = None
    return mock_plugin


def test_plugin_start_stop(mock_plugin):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    plugin_handler.start()
    plugin_handler.stop()
    mock_plugin_m.Plugin.assert_called_once_with({"foo": 1})
    mock_plugin = mock_plugin_m.Plugin(None)
    mock_plugin.setup.assert_called_once_with()
    mock_plugin.stop.assert_called_once_with()
    mock_plugin.process.assert_not_called()


def test_plugin_put(mock_plugin):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    plugin_handler.start()
    plugin_handler.put((metadata, 1))
    plugin_handler.stop()
    mock_plugin.process.assert_called_once_with(
        local_path="bar/relative_path/filename",
        metadata=metadata,
        data=1)


def test_plugin_fail_init(mock_plugin, caplog):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    mock_plugin_m.Plugin.side_effect = Exception()
    plugin_handler.start()
    plugin_handler.put((metadata, 1))
    plugin_handler.stop(0)
    mock_plugin.process.assert_not_called()
    assert "ERROR" in caplog.text


def test_plugin_fail_setup(mock_plugin, caplog):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    mock_plugin.setup.side_effect = Exception()
    plugin_handler.start()
    plugin_handler.put((metadata, 1))
    plugin_handler.stop(0)
    mock_plugin.process.assert_not_called()
    assert "ERROR" in caplog.text


def test_plugin_fail_process(mock_plugin, caplog):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    mock_plugin.process.side_effect = Exception()
    plugin_handler.start()
    plugin_handler.put((metadata, 1))
    plugin_handler.stop()
    mock_plugin.stop.assert_called_once_with()
    assert "ERROR" in caplog.text


def test_plugin_fail_stop(mock_plugin, caplog):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    mock_plugin.stop.side_effect = Exception()
    plugin_handler.start()
    plugin_handler.put((metadata, 1))
    plugin_handler.stop()
    assert "ERROR" in caplog.text


def test_plugin_slow_init(mock_plugin, caplog):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    mock_plugin_m.Plugin.side_effect = lambda *args, **kwargs: time.sleep(100)
    t0 = time.time()
    plugin_handler.start()
    plugin_handler.put((metadata, 1))
    plugin_handler.stop(0)
    mock_plugin.process.assert_not_called()
    assert time.time() - t0 < 10
    assert "timeout" in caplog.text


def test_plugin_slow_setup(mock_plugin, caplog):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    mock_plugin.setup.side_effect = lambda *args, **kwargs: time.sleep(100)
    t0 = time.time()
    plugin_handler.start()
    plugin_handler.put((metadata, 1))
    plugin_handler.stop(0)
    mock_plugin.process.assert_not_called()
    assert time.time() - t0 < 10
    assert "timeout" in caplog.text


def test_plugin_slow_process(mock_plugin, caplog):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    mock_plugin.process.side_effect = lambda *args, **kwargs: time.sleep(100)
    t0 = time.time()
    plugin_handler.start()
    plugin_handler.put((metadata, 1))
    plugin_handler.stop(0)
    assert time.time() - t0 < 10
    assert "timeout" in caplog.text


def test_plugin_slow_stop(mock_plugin, caplog):
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    mock_plugin.stop.side_effect = lambda *args, **kwargs: time.sleep(100)
    t0 = time.time()
    plugin_handler.start()
    plugin_handler.put((metadata, 1))
    plugin_handler.stop(0)
    assert time.time() - t0 < 10
    assert "timeout" in caplog.text


def test_plugin_queue_overlow(mock_plugin, caplog):
    """
    The queue size limit is not exceeded and put doesn't block when the limit
    is reached
    """
    plugin_handler = PluginHandler("mock_plugin", {"foo": 1}, "bar", log)
    mock_plugin.process.side_effect = lambda *args, **kwargs: time.sleep(100)
    t0 = time.time()
    plugin_handler.start()
    for i in range(10005):
        plugin_handler.put((metadata, 1))
    plugin_handler.stop(0)
    assert time.time() - t0 < 10
    assert plugin_handler.plugin_queue.qsize() == 10000
    assert "Skipping" in caplog.text
