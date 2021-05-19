import logging
from pathlib import Path
from unittest.mock import create_autospec
import zmq
import pytest

from hidra.transfer import Transfer, generate_filepath, UsageError

logging.getLogger("Transfer").setLevel(logging.DEBUG)


@pytest.fixture
def transfer():
    transfer = Transfer(
        connection_type="STREAM",
        signal_host=None,
        use_log=True,
        context="fake_context",
        dirs_not_to_create=[
            "commissioning/raw", "commissioning/scratch_bl",
            "current/raw", "current/scratch_bl", "local"]
    )
    transfer.confirmation_socket = create_autospec(zmq.Socket, instance=True)
    return transfer


@pytest.fixture
def metadata():
    return {
        "chunk_number": 0,
        "chunksize": 10485760,
        "relative_path": "current/raw",
        "confirmation_required": False,
        "version": "4.0.7",
        "filename": "test_filepath.tif",
        "file_mod_time": 1620136546,
        "filesize": 600
    }


@pytest.fixture
def current_raw(tmp_path):
    current_raw = tmp_path / "current/raw"
    current_raw.mkdir(parents=True)
    return current_raw


def test_generate_filepath(tmp_path, current_raw, metadata):
    filepath = generate_filepath(str(tmp_path), metadata)
    assert filepath == str(tmp_path / "current/raw/test_filepath.tif")


def test_store_chunk_single(transfer, tmp_path, current_raw, metadata):
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    descriptors = {}
    ret = transfer.store_chunk(
        descriptors, filepath, payload, str(tmp_path), metadata)
    assert not ret
    assert not descriptors
    assert Path(filepath).is_file()
    assert Path(filepath).read_bytes() == payload


def test_store_chunk_multiple(transfer, tmp_path, current_raw, metadata):
    metadata["chunksize"] = 6
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    metadata["filesize"] = len(payload)
    descriptors = {}
    for i in range(100):
        metadata["chunk_number"] = i
        ret = transfer.store_chunk(
            descriptors, filepath, payload[(i * 6):(i * 6 + 6)], str(tmp_path),
            metadata)
        if i < 99:
            assert ret
        else:
            assert not ret
    assert not descriptors
    assert Path(filepath).is_file()
    assert Path(filepath).read_bytes() == payload


def test_store_chunk_multiple_files(transfer, tmp_path, current_raw, metadata):
    metadata["chunksize"] = 6
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    metadata["filesize"] = len(payload)

    metadata2 = metadata.copy()
    metadata2["filename"] = "another_file.tif"
    filepath2 = generate_filepath(str(tmp_path), metadata2)
    payload2 = b"quxbaz" * 100

    descriptors = {}
    for i in range(100):
        metadata["chunk_number"] = i
        ret = transfer.store_chunk(
            descriptors, filepath, payload[(i * 6):(i * 6 + 6)], str(tmp_path),
            metadata)
        metadata2["chunk_number"] = i
        ret2 = transfer.store_chunk(
            descriptors, filepath2, payload2[(i * 6):(i * 6 + 6)],
            str(tmp_path), metadata2)
        if i < 99:
            assert ret
            assert ret2
        else:
            assert not ret
            assert not ret2
    assert not descriptors
    assert Path(filepath).is_file()
    assert Path(filepath).read_bytes() == payload
    assert Path(filepath2).is_file()
    assert Path(filepath2).read_bytes() == payload2


def test_store_chunk_dirs_not_to_create(
        transfer, tmp_path, current_raw, metadata):
    current_raw.rmdir()
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    descriptors = {}
    with pytest.raises(IOError):
        transfer.store_chunk(
            descriptors, filepath, payload, str(tmp_path), metadata)

    assert not descriptors


def test_store_chunk_dirs_no_current(
        transfer, tmp_path, metadata):
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    descriptors = {}
    with pytest.raises(IOError):
        transfer.store_chunk(
            descriptors, filepath, payload, str(tmp_path), metadata)

    assert not descriptors


def test_store_chunk_perission_denied(
        transfer, tmp_path, current_raw, metadata, caplog):
    current_raw.chmod(0o400)
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    descriptors = {}
    with pytest.raises(IOError):
        transfer.store_chunk(
            descriptors, filepath, payload, str(tmp_path), metadata)

    assert not descriptors
    assert "Failed to open file" in caplog.text


def test_store_chunk_subdir(transfer, tmp_path, current_raw, metadata):
    metadata["relative_path"] = "current/raw/subdir"
    filepath = generate_filepath(str(tmp_path), metadata)
    assert filepath == str(tmp_path / "current/raw/subdir/test_filepath.tif")
    payload = b"foobar" * 100
    descriptors = {}
    ret = transfer.store_chunk(
        descriptors, filepath, payload, str(tmp_path), metadata)
    assert not ret
    assert not descriptors
    assert Path(filepath).is_file()
    assert Path(filepath).read_bytes() == payload


@pytest.mark.parametrize("error", [KeyboardInterrupt, OSError])
def test_store_chunk_write_error(
        transfer, tmp_path, current_raw, metadata, error):
    filepath = generate_filepath(str(tmp_path), metadata)
    metadata["chunksize"] = 6
    payload = b"foobar" * 100
    metadata["filesize"] = len(payload)
    descriptors = {}

    # Send one chunk to populate descriptors
    metadata["chunk_number"] = 0

    ret = transfer.store_chunk(
        descriptors, filepath, payload[0:6], str(tmp_path),
        metadata)
    assert ret
    assert descriptors

    # Throw error while writing
    metadata["chunk_number"] = 1

    def dummy_write(data):
        raise error

    descriptors[filepath]["file"].write = dummy_write
    with pytest.raises(error):
        transfer.store_chunk(
            descriptors, filepath, payload, str(tmp_path), metadata)

    assert not descriptors
    assert Path(filepath).is_file()


def test_store_chunk_send_chunk_twice(
        transfer, tmp_path, current_raw, metadata):
    # Can this actually happen?
    filepath = generate_filepath(str(tmp_path), metadata)
    metadata["chunksize"] = 6
    payload = b"foobar" * 3
    metadata["filesize"] = len(payload)
    descriptors = {}

    # Send first chunk twice
    metadata["chunk_number"] = 0

    ret = transfer.store_chunk(
        descriptors, filepath, payload[0:6], str(tmp_path),
        metadata)

    assert ret
    assert descriptors

    ret = transfer.store_chunk(
        descriptors, filepath, payload[0:6], str(tmp_path),
        metadata)

    assert ret
    assert descriptors

    # Send second chunk twice
    metadata["chunk_number"] = 1

    ret = transfer.store_chunk(
        descriptors, filepath, payload[0:6], str(tmp_path),
        metadata)

    assert ret
    assert descriptors

    ret = transfer.store_chunk(
        descriptors, filepath, payload[0:6], str(tmp_path),
        metadata)

    assert ret
    assert descriptors

    # Last chunk can only be sent once
    metadata["chunk_number"] = 2

    ret = transfer.store_chunk(
        descriptors, filepath, payload[0:6], str(tmp_path),
        metadata)

    assert not ret
    assert not descriptors
    assert Path(filepath).is_file()
    assert Path(filepath).read_bytes() == payload


def test_store_chunk_resend_first_chunk(
        transfer, tmp_path, current_raw, metadata):
    # Can this actually happen?
    filepath = generate_filepath(str(tmp_path), metadata)
    metadata["chunksize"] = 6
    payload = b"foobar" * 3
    metadata["filesize"] = len(payload)
    descriptors = {}

    # Send first two chunks
    for i in range(2):
        metadata["chunk_number"] = i

        ret = transfer.store_chunk(
            descriptors, filepath, payload[0:6], str(tmp_path),
            metadata)

        assert ret
        assert descriptors

    # Send all chunks starting again from 0
    for i in range(3):
        metadata["chunk_number"] = i

        ret = transfer.store_chunk(
            descriptors, filepath, payload[0:6], str(tmp_path),
            metadata)

        if i < 2:
            assert ret
            assert descriptors

    assert not ret
    assert not descriptors
    assert Path(filepath).is_file()
    assert Path(filepath).read_bytes() == payload


def test_store_chunk_overwrite_same_file(
        transfer, tmp_path, current_raw, metadata):
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    descriptors = {}
    ret = transfer.store_chunk(
        descriptors, filepath, payload, str(tmp_path), metadata)
    assert not ret
    assert not descriptors
    assert Path(filepath).is_file()
    assert Path(filepath).read_bytes() == payload

    payload = b"overwritten" * 100
    metadata["filesize"] = len(payload)
    ret = transfer.store_chunk(
        descriptors, filepath, payload, str(tmp_path), metadata)
    assert not ret
    assert not descriptors
    assert Path(filepath).is_file()
    assert Path(filepath).read_bytes() == payload


def test_store_chunk_confirmation_not_enabled(
        transfer, tmp_path, current_raw, metadata):
    # How can this happen?
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    descriptors = {}
    metadata["confirmation_required"] = "test_topic"
    transfer.confirmation_socket = None

    with pytest.raises(UsageError):
        transfer.store_chunk(
            descriptors, filepath, payload, str(tmp_path), metadata)

    assert descriptors  # Is this intended?


def test_store_chunk_confirmation_old(
        transfer, tmp_path, current_raw, metadata):
    # How can this happen?
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    metadata["chunksize"] = 6
    descriptors = {}
    metadata["confirmation_required"] = "test_topic"

    ret = transfer.store_chunk(
        descriptors, filepath, payload[0:6], str(tmp_path), metadata)

    assert descriptors
    assert ret

    file_id = str(Path(metadata["relative_path"]) / metadata["filename"])
    transfer.confirmation_socket.send_multipart.assert_called_once_with([
        b"test_topic", file_id.encode()])


def test_store_chunk_confirmation_new(
        transfer, tmp_path, current_raw, metadata):
    # How can this happen?
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    metadata["chunksize"] = 6
    descriptors = {}
    metadata["confirmation_required"] = "test_topic"
    metadata["version"] = "4.0.8"  # First version confirming the chunk number

    ret = transfer.store_chunk(
        descriptors, filepath, payload[0:6], str(tmp_path), metadata)

    assert descriptors
    assert ret

    file_id = str(Path(metadata["relative_path"]) / metadata["filename"])
    transfer.confirmation_socket.send_multipart.assert_called_once_with([
        b"test_topic", file_id.encode(), b"0"])


def test_store_chunk_confirmation_error(
        transfer, tmp_path, current_raw, metadata):
    filepath = generate_filepath(str(tmp_path), metadata)
    payload = b"foobar" * 100
    descriptors = {}
    metadata["confirmation_required"] = "test_topic"
    transfer.confirmation_socket.send_multipart.side_effect = OSError

    with pytest.raises(OSError):
        transfer.store_chunk(
            descriptors, filepath, payload, str(tmp_path), metadata)

    assert descriptors  # Is this intended?
    pytest.skip()  # Probably the following should be true
    assert Path(filepath).is_file()
