import concurrent
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import re
import subprocess
import time
import pytest

thread_pool = ThreadPoolExecutor()

hidra_testdir = Path(os.environ["HIDRA_TESTDIR"])
receiver_beamline = hidra_testdir / Path("receiver/beamline/p00")


def parse_output(string):
    d = {}
    for line in string.split("\n"):
        if line:
            key, value = line.split(":")
            d[key.strip()] = value.strip()
    return d


def calc_hash(file):
    with open(file, "rb") as f:
        data = f.read()
    md5sum = hashlib.md5()
    md5sum.update(data)
    return md5sum.hexdigest()


def start_process(cmd):
    process = subprocess.Popen(
        cmd,
        universal_newlines=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    return process


class TimeoutTimer:
    def __init__(self, timeout):
        self.start = time.time()
        self.timeout = timeout

    def elapsed(self):
        return time.time() - self.start

    def remaining(self):
        return self.timeout - self.elapsed()

    def has_expired(self):
        return self.remaining() <= 0


def wait_for(predicate, timeout=10):
    if predicate():
        return True

    timer = TimeoutTimer(timeout)

    while not timer.has_expired():
        if predicate():
            return True

    return predicate()


def wait_for_output(file, pattern, timeout=10):
    """Wait for pattern in output from a pipe

    Returns early if the specified pattern was found or EOF was reached.

    Parameters
    ----------
    file: file-like object
        For example, the stdout of a running process
    pattern: str
        A regular expression that should be matched in a line from the output
    timeout: float (default: 10)
        Wait at most timeout seconds

    Returns
    -------
    success: bool
        True, if pattern was found, False otherwise
    output: str
        Output so far
    """
    timer = TimeoutTimer(timeout)
    output = []
    regex = re.compile(pattern)
    found = False
    while not timer.has_expired():
        future = thread_pool.submit(file.readline)
        try:
            line = future.result(timer.remaining())
        except concurrent.futures.TimeoutError:
            break
        if not line:
            break
        output.append(line)
        if regex.match(line):
            found = True
            break
    return found, "".join(output)


def docker_start(name, cmd):
    full_cmd = [
        "docker", "exec", "--env", "TERM=linux", name] + cmd
    return start_process(full_cmd)


def docker_run(name, cmd):
    full_cmd = [
        "docker", "exec", "--env", "TERM=linux", name] + cmd
    return subprocess.run(
        full_cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)


def control_client(cmd, beamline=None, det=None, detapi="1.6.0"):
    full_cmd = [
        "/opt/hidra/src/hidra/hidra_control/client.py", "--{}".format(cmd)]

    if beamline:
        full_cmd += ["--beamline", beamline]

    if det:
        full_cmd += ["--det", det]

    full_cmd += ["--detapi", detapi]

    return docker_run("control-client", full_cmd)


def create_eiger_files(
        number=1, size=1000000, path="current/raw", prefix="test_", ext="h5"):
    cmd = [
        "python", "/app/create_files.py",
        "--number", str(number),
        "--size", str(size),
        "--path", str(path),
        "--prefix", str(prefix),
        "--ext", str(ext)]
    out = docker_run("eiger", cmd)
    try:
        hashes = parse_output(out.stdout)
    except ValueError:
        print(out.stdout)
        print(out.stderr)
    return hashes


@contextmanager
def start_transfer_client(
        signal_host="sender",
        target_host="transfer-client",
        detector_id=None,
        query_type="QUERY_NEXT",
        max_queries=None,
        timeout=2000):
    cmd = [
        "python", "/app/check_query.py",
        "--signal_host", str(signal_host),
        "--target_host", str(target_host),
        "--detector_id", str(detector_id),
        "--query_type", str(query_type),
        "--max_queries", str(max_queries),
        "--timeout", str(timeout)]
    proc = docker_start("transfer-client", cmd)
    yield proc
    proc.terminate()
    proc.wait(5)
    proc.kill()


def stop_sender_script(sender):
    out = docker_run(
        sender, ["/opt/hidra/hidra.sh", "stop", "--beamline", "p00"])
    return out


def start_sender_script(sender):
    out = docker_run(
        sender, ["/opt/hidra/hidra.sh", "start", "--beamline", "p00"])
    return out


def stop_sender_systemctl(sender):
    out = docker_run(
        sender, ["systemctl", "stop", "hidra@p00"])
    return out


def start_sender_systemctl(sender):
    out = docker_run(
        sender, ["systemctl", "start", "hidra@p00"])
    return out


def stop_sender(sender_type):
    if sender_type in ["sender-freeze", "sender-suse"]:
        out = stop_sender_script(sender_type)
    elif sender_type in ["sender-debian", "sender-debian10"]:
        out = stop_sender_systemctl(sender_type)
        assert out.returncode == 0
    else:
        raise ValueError("Sender type not supported")
    return out


def start_sender(
        sender_type="sender-freeze", eventdetector_type="inotify_events"):
    senders = [
        "sender-freeze", "sender-debian", "sender-debian10", "sender-suse"]
    if sender_type not in senders:
        raise ValueError("Sender not supported")
    event_types = ["inotify_events", "inotifyx_events", "watchdog_events"]
    if eventdetector_type not in event_types:
        raise ValueError("Event type not supported")

    out = docker_run(
        sender_type, [
            "sed", "-i", "-r",
            "s/(\\s*type:\\s*)({})/\\1{}/g".format(
                "|".join(event_types), eventdetector_type),
            "/opt/hidra/conf/datamanager_p00.yaml"])
    print("sed:", out.stdout, out.stderr)
    assert out.returncode == 0

    if sender_type in ["sender-freeze", "sender-suse"]:
        out = start_sender_script(sender_type)
        print("start:", out.stdout, out.stderr)
        assert "OK" in out.stdout or "..done" in out.stdout
    elif sender_type in ["sender-debian", "sender-debian10"]:
        out = start_sender_systemctl(sender_type)
        assert out.returncode == 0

    proc = docker_start(
        sender_type,
        ["timeout", "10", "tail", "-f", "/var/log/hidra/datamanager_p00.log"])
    success, output = wait_for_output(
        proc.stdout, "Waiting for new job", timeout=10)
    proc.terminate()
    print("waiting for tail")
    comm = proc.communicate(timeout=5)
    if not success:
        print("comm:", comm)
        print("output:", output)
    print("done")
    return out


@pytest.fixture(
    scope="module",
    params=[
        "sender-freeze", "sender-debian", "sender-debian10", "sender-suse"])
def sender_type(request):
    return request.param


@pytest.fixture(
    scope="module",
    params=["inotify_events", "inotifyx_events", "watchdog_events"])
def eventdetector_type(request):
    return request.param


@pytest.fixture(scope="module")
def stopped_sender_instance(sender_type, eventdetector_type):
    if (
            sender_type in ["sender-debian10"]
            and eventdetector_type == "inotifyx_events"):
        pytest.skip()
    stop_sender(sender_type)
    return {
        "sender_type": sender_type, "eventdetector_type": eventdetector_type}


@pytest.fixture(scope="module")
def sender_instance(stopped_sender_instance):
    sender_type = stopped_sender_instance["sender_type"]
    eventdetector_type = stopped_sender_instance["eventdetector_type"]
    start_sender(
        sender_type=sender_type, eventdetector_type=eventdetector_type)
    yield stopped_sender_instance
    out = stop_sender(sender_type)
    print(out.stdout)
    print(out.stderr)


@pytest.fixture(scope="module")
def stopped_eiger_instance():
    control_client("stop", beamline="p00", det="eiger")


@pytest.fixture(
    scope="module",
    params=["1.6.0", "1.8.0"])
def eiger_instance(stopped_eiger_instance, request):
    detapi = request.param
    control_client("start", beamline="p00", det="eiger", detapi=detapi)
    yield {"detapi": detapi}
    control_client("stop", beamline="p00", det="eiger")


def test_control_status_stop():
    control_client("start", beamline="p00", det="eiger")
    control_client("stop", beamline="p00", det="eiger")
    ret = control_client("status", beamline="p00", det="eiger")
    assert " NOT_RUNNING" in ret.stdout


def test_control_status_start():
    control_client("stop", beamline="p00", det="eiger")
    control_client("start", beamline="p00", det="eiger")
    ret = control_client("status", beamline="p00", det="eiger")
    assert " RUNNING" in ret.stdout


def test_control_eiger_getsettings(eiger_instance):
    ret = control_client("getsettings", beamline="p00", det="eiger")
    settings = parse_output(ret.stdout)
    assert settings["Detector IP"] == "eiger"
    assert settings["Detector API version"] == eiger_instance["detapi"]
    assert settings["History size"] == "2000"
    assert settings["Store data"] == "True"
    assert settings["Remove data from the detector"] == "True"
    assert settings["Whitelist"] == "a3p00-hosts"
    assert settings["Ldapuri"] == "ldap.hidra.test"


def test_control_eiger_store_files(eiger_instance):
    created_hashes = create_eiger_files(
        number=1, prefix="eiger_store_files_{}-".format(
            eiger_instance["detapi"]),
        ext="h5")

    for source_file, hash in created_hashes.items():
        target_file = receiver_beamline / Path(source_file)
        assert wait_for(target_file.is_file)
        assert wait_for(lambda: hash == calc_hash(target_file))


def test_transfer_after_restart():
    control_client("start", beamline="p00", det="eiger")
    control_client("stop", beamline="p00", det="eiger")
    control_client("start", beamline="p00", det="eiger")
    print("Datamanager started")
    with start_transfer_client(
            signal_host="receiver", detector_id="eiger",
            query_type="QUERY_NEXT_METADATA", max_queries=1,
            timeout=10000) as proc:
        success, output = wait_for_output(
            proc.stderr, r"Begin query...", timeout=30)
        print("stderr so far")
        print(output)
        if not success:
            print("Killing query")
            proc.terminate()
            stdout, stderr = proc.communicate(timeout=30)
            print("remaining stderr")
            print(stderr)
            print("remaining stdout")
            print(stdout)
            raise TimeoutError()

        print("Query started")
        created_hashes = create_eiger_files(
            number=1, prefix="transfer_after_restart", ext="cbf")

        print("Files created")
        stdout, stderr = proc.communicate(timeout=30)
        print("Query finished")
        print("stdout:")
        print(stdout)
        print("stderr:")
        print(output + stderr)
        received_hashes = parse_output(stdout)
    assert list(received_hashes.keys()) == list(created_hashes.keys())
    for key in created_hashes.keys():
        filename = receiver_beamline / Path(key)
        assert filename.is_file()


def test_sender_status_stopped(stopped_sender_instance):
    sender_type = stopped_sender_instance["sender_type"]
    if sender_type in ["sender-freeze", "sender-suse"]:
        out = docker_run(
            sender_type,
            ["/opt/hidra/hidra.sh", "status", "--beamline", "p00"])
        assert (
            "hidra_p00 is not running" in out.stdout
            or "hidra_p00 ..unused" in out.stdout)
    elif sender_type in ["sender-debian", "sender-debian10"]:
        out = docker_run(
            sender_type, ["systemctl", "is-active", "hidra@p00"])
        assert out.stdout == "inactive\n"


def test_sender_status_running(sender_instance):
    sender_type = sender_instance["sender_type"]
    if sender_type in ["sender-freeze", "sender-suse"]:
        out = docker_run(
            sender_type,
            ["/opt/hidra/hidra.sh", "status", "--beamline", "p00"])
        assert (
            "hidra_p00 is running" in out.stdout
            or "hidra_p00 ..running" in out.stdout)
    elif sender_type in ["sender-debian", "sender-debian10"]:
        out = docker_run(
            sender_type, ["systemctl", "is-active", "hidra@p00"])
        assert out.stdout == "active\n"


def test_sender_file_writing(sender_instance):
    sender_type = sender_instance["sender_type"]
    filename = Path("current/raw/filewriting_{}_{}.txt".format(
        sender_instance["sender_type"], sender_instance["eventdetector_type"]))
    sender_path = hidra_testdir / sender_type / "ramdisk" / filename
    sender_path.write_text("hello world")
    receiver_path = receiver_beamline / filename

    # first file can take longer
    assert wait_for(lambda: not sender_path.is_file(), timeout=60)
    assert wait_for(receiver_path.is_file)
    assert wait_for(lambda: receiver_path.read_text() == "hello world")
    stat = receiver_path.stat()
    # uid and gid are hard coded in receiver/Dockerfile
    assert stat.st_uid == 1234
    assert stat.st_gid == 1234

    # additional transfers should be fast
    sender_paths = []
    receiver_paths = []
    for i in range(100):
        sender_type = sender_instance["sender_type"]
        filename = Path("current/raw/filewriting_{}_{}_{}.txt".format(
            sender_instance["sender_type"],
            sender_instance["eventdetector_type"],
            i))
        sender_path = hidra_testdir / sender_type / "ramdisk" / filename
        sender_path.write_text("hello world" + str(i))
        receiver_path = receiver_beamline / filename
        sender_paths.append(sender_path)
        receiver_paths.append(receiver_path)

    assert wait_for(lambda: not sender_paths[0].is_file(), timeout=20)
    for i, (sender_path, receiver_path) in enumerate(
            zip(sender_paths, receiver_paths)):
        assert wait_for(lambda: not sender_path.is_file(), timeout=2)
        assert wait_for(receiver_path.is_file, timeout=1)
        assert wait_for(
            lambda: receiver_path.read_text() == "hello world" + str(i),
            timeout=1)
        stat = receiver_path.stat()
        # uid and gid are hard coded in receiver/Dockerfile
        assert stat.st_uid == 1234
        assert stat.st_gid == 1234


def test_receiver_groups():
    script = (
        "pids=$(pgrep hidra_p00 -d \" \"); for pid in ${pids}; do"
        " echo -n \"$pid: \";"
        " grep Groups /proc/$pid/status | grep -o -E \"[0-9 ]*\"; done")
    out = docker_run("asap3-p00", ["sh", "-c", script])
    for line in out.stdout.strip().split("\n"):
        print(line)
        pid, groups = line.split(":")
        # gids are hard coded in receiver/Dockerfile
        assert "1234" in groups
        assert "2345" in groups
