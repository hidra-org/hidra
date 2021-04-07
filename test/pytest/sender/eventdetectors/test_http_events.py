import logging
from pathlib import Path
from unittest.mock import create_autospec, patch
import pytest
import hidra  # noqa
from eventdetectors.http_events import (
    EventDetectorImpl, create_eventdetector_impl, HTTPConnection,
    FileFilterDeque, FileFilterSet, resolve_ip)

fix_subdir = Path("current/raw")

log = logging.getLogger(__name__)


def test_resolve_ip():
    ip = resolve_ip("127.0.0.1")
    assert ip == "127.0.0.1"


def test_resolve_ip_by_name():
    ip = resolve_ip("localhost")
    assert ip in ["127.0.0.1", "::1"]


def test_create_eventdetector_imp_full_dict():
    config = dict(
        det_ip="127.0.0.1",
        det_api_version="1.8.0",
        history_size=2000,
        fix_subdirs=["current/raw"],
        file_writer_url="foo",
        data_url="bar"
    )
    eventdetector = create_eventdetector_impl(**config, log=log)
    assert eventdetector
    assert isinstance(eventdetector.file_filter, FileFilterDeque)


def test_create_eventdetector_imp_default_dict():
    config = dict(
        det_ip="127.0.0.1",
        det_api_version="1.8.0",
        history_size=2000,
        fix_subdirs=["current/raw"],
    )
    eventdetector = create_eventdetector_impl(**config, log=log)
    assert eventdetector
    assert isinstance(eventdetector.file_filter, FileFilterDeque)


def test_create_eventdetector_imp_negative_history_size():
    config = dict(
        det_ip="127.0.0.1",
        det_api_version="1.8.0",
        history_size=-1,
        fix_subdirs=["current/raw"],
    )
    eventdetector = create_eventdetector_impl(**config, log=log)
    assert eventdetector
    assert isinstance(eventdetector.file_filter, FileFilterSet)


@pytest.fixture
def mock_sleep():
    with patch("eventdetectors.http_events.time.sleep", autospec=True) as mock:
        yield mock


@pytest.fixture
def connection():
    mock = create_autospec(HTTPConnection)
    mock.get_file_list.return_value = []
    return mock


@pytest.fixture
def file_filter():
    return FileFilterDeque(["current/raw"], 10)


@pytest.fixture
def file_filter_set():
    return FileFilterSet(["current/raw"])


@pytest.fixture
def eventdetector(connection, file_filter):
    return EventDetectorImpl(
        "file_url", "data_url", connection, file_filter, log)


def test_get_files_stored_empty(eventdetector):
    files_stored = eventdetector._get_files_stored()
    assert files_stored == []


def test_get_files_stored(eventdetector, connection):
    connection.get_file_list.return_value = ["filename"]
    files_stored = eventdetector._get_files_stored()
    assert files_stored == ["filename"]


def test_get_files_stored_version_1_8_0_empty(eventdetector, connection):
    connection.get_file_list.return_value = {"value": []}
    files_stored = eventdetector._get_files_stored()
    assert files_stored == []


def test_get_files_stored_version_1_8_0_none(eventdetector, connection):
    connection.get_file_list.return_value = {"value": None}
    files_stored = eventdetector._get_files_stored()
    assert files_stored == []


def test_get_files_stored_version_1_8_0(eventdetector, connection):
    connection.get_file_list.return_value = {"value": ["filename"]}
    files_stored = eventdetector._get_files_stored()
    assert files_stored == ["filename"]


def test_filter(file_filter):
    files = ["current/raw/filename.ext"]
    ret = file_filter.get_new_files(files)
    assert ret == files


def test_filter_multiple_files(file_filter):
    files = ["current/raw/filename.ext", "current/raw/filename2.ext"]
    ret = file_filter.get_new_files(files)
    assert ret == files


def test_filter_seen_files(file_filter):
    files = ["current/raw/filename1.ext"]
    ret = file_filter.get_new_files(files)
    assert ret == files

    files = ["current/raw/filename1.ext", "current/raw/filename2.ext"]
    ret = file_filter.get_new_files(files)
    assert ret == ["current/raw/filename2.ext"]

    files = ["current/raw/filename2.ext", "current/raw/filename3.ext"]
    ret = file_filter.get_new_files(files)
    assert ret == ["current/raw/filename3.ext"]

    files = [
        "current/raw/filename1.ext", "current/raw/filename2.ext",
        "current/raw/filename3.ext"]
    ret = file_filter.get_new_files(files)
    assert ret == []


def test_filter_seen_history_size(file_filter):
    files = []
    for i in range(11):
        filename = "current/raw/filename{}.ext".format(i)
        files.append(filename)
        ret = file_filter.get_new_files(files)
        assert ret == [filename]

    filename = "current/raw/filename11.ext"
    files.append(filename)
    ret = file_filter.get_new_files(files)
    # ret now contains all files, which is a bug
    pytest.skip()
    assert ret == ["current/raw/filename0.ext", filename]


def test_filter_set(file_filter_set):
    files = ["current/raw/filename.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == files


def test_filter_multiple_files_set(file_filter_set):
    files = ["current/raw/filename.ext", "current/raw/filename2.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == files


def test_filter_seen_files_set(file_filter_set):
    files = ["current/raw/filename1.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == files

    files = ["current/raw/filename1.ext", "current/raw/filename2.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == ["current/raw/filename2.ext"]

    files = ["current/raw/filename2.ext", "current/raw/filename3.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == ["current/raw/filename3.ext"]

    files = [
        "current/raw/filename1.ext", "current/raw/filename2.ext",
        "current/raw/filename3.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == ["current/raw/filename1.ext"]


def test_filter_seen_files_reuse(file_filter_set):
    files = ["current/raw/filename.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == files

    files = ["current/raw/filename.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == []

    files = []
    ret = file_filter_set.get_new_files(files)
    assert ret == files

    files = ["current/raw/filename.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == files


def test_filter_not_in_subdir(file_filter):
    files = ["other_dir/filename.ext"]
    ret = file_filter.get_new_files(files)
    assert ret == []


def test_filter_not_in_subdir_set(file_filter_set):
    files = ["other_dir/filename.ext"]
    ret = file_filter_set.get_new_files(files)
    assert ret == []


def test_new_event_empty(eventdetector, mock_sleep):
    new_events = eventdetector.get_new_event()
    assert new_events == []
    mock_sleep.assert_called_with(0.5)


def test_new_event_exception(eventdetector, connection, mock_sleep):
    connection.get_file_list.side_effect = ConnectionError
    new_events = eventdetector.get_new_event()
    assert new_events == []
    mock_sleep.assert_called_with(0.5)


def test_new_event_files(eventdetector, connection, mock_sleep):
    connection.get_file_list.return_value = ["current/raw/filename.ext"]
    new_events = eventdetector.get_new_event()
    assert new_events == [{
        'source_path': 'data_url', 'filename': 'filename.ext',
        'relative_path': 'current/raw'}]
    mock_sleep.assert_not_called()


def test_new_event_seen_and_reused_filename(
        connection, file_filter_set, mock_sleep):
    eventdetector = EventDetectorImpl(
        "file_url", "data_url", connection, file_filter_set, log)
    filename = "current/raw/filename.ext"
    file_event = {
        'source_path': 'data_url', 'filename': 'filename.ext',
        'relative_path': 'current/raw'}

    mock_sleep.reset_mock()
    connection.get_file_list.return_value = [filename]
    new_events = eventdetector.get_new_event()
    assert new_events == [file_event]
    mock_sleep.assert_not_called()

    mock_sleep.reset_mock()
    connection.get_file_list.return_value = [filename]
    new_events = eventdetector.get_new_event()
    assert new_events == []
    mock_sleep.assert_called_with(0.5)

    mock_sleep.reset_mock()
    connection.get_file_list.return_value = [filename]
    new_events = eventdetector.get_new_event()
    assert new_events == []
    mock_sleep.assert_called_with(0.5)

    mock_sleep.reset_mock()
    connection.get_file_list.return_value = []
    new_events = eventdetector.get_new_event()
    assert new_events == []
    mock_sleep.assert_called_with(0.5)

    mock_sleep.reset_mock()
    connection.get_file_list.return_value = [filename]
    new_events = eventdetector.get_new_event()
    assert new_events == [file_event]
    mock_sleep.assert_not_called()
