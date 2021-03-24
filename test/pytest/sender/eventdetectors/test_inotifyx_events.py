import os
from pathlib import Path
import pytest
import hidra  # noqa
from eventdetectors.inotifyx_events import EventDetector

fix_subdir = Path("current/raw")


@pytest.fixture
def create_eventdetector(tmp_path):
    ed = None

    def create_ed(
            monitored_events={"IN_CLOSE_WRITE": [""]},
            event_timeout=2):
        nonlocal ed
        if ed is not None:
            raise RuntimeError(
                "Only one event detector can be created in each test")

        ed = EventDetector({
            "config": {
                # only extension matters in config_file name
                "general": {"config_file": "foo.yaml"},
                "eventdetector": {
                    "type": "inotifyx_events",
                    "dirs_not_to_create": [],
                    "inotifyx_events": {
                        "monitored_dir": str(tmp_path),
                        "fix_subdirs": [str(fix_subdir)],
                        "create_fix_subdirs": True,
                        "monitored_events": monitored_events,
                        "use_cleanup": False,
                        "history_size": 0,
                        "event_timeout": event_timeout}}},
            "check_dep": True, "context": None, "log_queue": None})

        return ed

    yield create_ed

    ed.stop()


def test_plain_file(create_eventdetector, tmp_path):
    tmp_path = Path(str(tmp_path))  # workaround pytest issue
    eventdetector = create_eventdetector()

    filename = tmp_path / fix_subdir / "plain_file.txt"
    filename.write_text("foo")

    events = eventdetector.get_new_event()

    assert len(events) == 1
    event = events[0]
    assert event["filename"] == filename.name
    assert event["relative_path"] == str(fix_subdir)
    assert event["source_path"] == str(tmp_path)


@pytest.mark.parametrize(
    "subdir", ["subdir", "nested/subdir", "deeply/nested/subdir"])
def test_subdir(create_eventdetector, tmp_path, subdir):
    tmp_path = Path(str(tmp_path))  # workaround pytest issue
    eventdetector = create_eventdetector()

    filename = tmp_path / fix_subdir / subdir / "file_in_subdir.txt"
    filename.parent.mkdir(parents=True)
    filename.write_text("foo")

    events = eventdetector.get_new_event()

    assert len(events) == 1
    event = events[0]
    assert event["filename"] == filename.name
    assert event["relative_path"] == str(fix_subdir / subdir)
    assert event["source_path"] == str(tmp_path)


def test_multiple_events(create_eventdetector, tmp_path):
    tmp_path = Path(str(tmp_path))  # workaround pytest issue
    eventdetector = create_eventdetector()

    filenames = []
    for i in range(1000):
        filename = tmp_path / fix_subdir / "multiple_files_{}.txt".format(i)
        filename.write_text("foo")
        filenames.append(filename)

    events = eventdetector.get_new_event()

    assert len(events) == len(filenames)

    for i, filename in enumerate(filenames):
        event = events[i]
        assert event["filename"] == filename.name
        assert event["relative_path"] == str(fix_subdir)
        assert event["source_path"] == str(tmp_path)


def test_filter_extension(create_eventdetector, tmp_path):
    tmp_path = Path(str(tmp_path))  # workaround pytest issue
    eventdetector = create_eventdetector(
        monitored_events={"IN_CLOSE_WRITE": ["tif"]})

    filename = tmp_path / fix_subdir / "correct_extension.tif"
    filename.write_text("foo")

    filename_ignored = tmp_path / fix_subdir / "wrong_extension.txt"
    filename_ignored.write_text("foo")

    events = eventdetector.get_new_event()

    assert len(events) == 1
    event = events[0]
    assert event["filename"] == filename.name
    assert event["relative_path"] == str(fix_subdir)
    assert event["source_path"] == str(tmp_path)


def test_filter_event_type(create_eventdetector, tmp_path):
    tmp_path = Path(str(tmp_path))  # workaround pytest issue

    eventdetector = create_eventdetector(
        monitored_events={"IN_MOVED_TO": [""]})

    filename_ignored = tmp_path / fix_subdir / "closed.tif"
    filename_ignored.write_text("foo")

    filename_ignored = tmp_path / fix_subdir / "moved.txt"
    filename_ignored.write_text("foo")
    filename = filename_ignored.with_suffix(".tif")
    os.rename(str(filename_ignored), str(filename))

    events = eventdetector.get_new_event()

    assert len(events) == 1
    event = events[0]
    assert event["filename"] == filename.name
    assert event["relative_path"] == str(fix_subdir)
    assert event["source_path"] == str(tmp_path)


def test_timeout(create_eventdetector):
    eventdetector = create_eventdetector(event_timeout=0.1)

    events = eventdetector.get_new_event()

    assert events == []
