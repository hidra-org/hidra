from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import random
import signal
import socket
from threading import Event
from time import sleep
import yaml

from hidra import Transfer, __version__, generate_filepath, _constants
from plugins.asapo_producer import AsapoWorker

logger = logging.getLogger(__name__)


class Stopped(Exception):
    """Raised when a run is stopped."""
    pass


class TransferConfig:
    def __init__(self, signal_host, target_host, detector_id,
                 endpoint, beamline, default_data_source, token,
                 target_dir='', n_threads=1, start_file_idx=1, beamtime='auto',
                 file_regex="current/raw/(?P<scan_id>.*)_(?P<file_idx_in_scan>.*).h5",
                 timeout=30, reconnect_timeout=3, log_level="INFO"):
        self.detector_id = detector_id
        self.log_level = log_level

        self.endpoint = endpoint
        self.beamtime = beamtime
        self.beamline = beamline
        self.token = token
        self.n_threads = n_threads
        self.timeout = timeout
        self.default_data_source = default_data_source
        self.file_regex = file_regex
        self.start_file_idx = start_file_idx

        self.signal_host = signal_host
        self.target_host = target_host
        self.target_dir = target_dir
        self.reconnect_timeout = reconnect_timeout

    def __repr__(self):
        return str(self.__dict__)


def read_token(token_path, beamline):
    with open("{}/{}.token".format(token_path, beamline), "r") as f:
        token = f.readline().split("\n")[0]
    return token


def create_query(signal_host, detector_id):
    logger.info(
        "Creating Transfer instance type=%s signal_host=%s detector_id=%s use_log=True",
        "STREAM_METADATA", signal_host, detector_id)
    query = Transfer("STREAM_METADATA", signal_host, detector_id=detector_id,
                     use_log=True)
    return query


def create_asapo_transfer(asapo_worker, query, target_host, target_dir, reconnect_timeout, stop_event):
    """Create an AsapoTransfer object with a random port"""
    target_port = random.randrange(50101, 50200)
    asapo_transfer = AsapoTransfer(
        asapo_worker=asapo_worker,
        query=query,
        target_host=target_host,
        target_port=target_port,
        target_dir=target_dir,
        reconnect_timeout=reconnect_timeout,
        stop_event=stop_event,
    )

    return asapo_transfer


class AsapoTransfer:
    def __init__(self, asapo_worker, query, target_host, target_port, target_dir, reconnect_timeout, stop_event=None):
        self.query = query
        self.target_host = target_host
        self.target_port = target_port
        self.target_dir = target_dir
        self.asapo_worker = asapo_worker
        self.reconnect_timeout = reconnect_timeout
        if stop_event is None:
            stop_event = Event()
        self.stop_run = stop_event

    def run(self):
        if self.stop_run.is_set():
            raise Stopped
        try:
            # All interactions with the query object have to happen in this
            # block to ensure query.stop is always called
            logger.info("Starting query")
            self.query.start([self.target_host, self.target_port])
            logger.info("Initiating connection for %s : %s", self.target_host, self.target_port)
            self.query.initiate([[self.target_host, self.target_port, 1]])
            self._run()
        finally:
            logger.info("Stopping query")
            # Calling stop might raise. The caller should discard the query
            # object in any case after this and therefore exceptions from stop
            # don't need to be handled specifically.
            self.query.stop()

    def _run(self):
        while not self.stop_run.is_set():
            logger.debug("Querying next message")
            [metadata, data] = self.query.get(timeout=self.reconnect_timeout*1000)
            if metadata is not None:
                try:
                    local_path = generate_filepath(self.target_dir, metadata)
                    logger.info("Send file {local_path}".format(local_path=local_path))
                    self.asapo_worker.send_message(local_path, metadata)
                except Exception as e:
                    logger.error("Transmission does not succeed. {e}. File ignored".format(e=str(e)))

    def stop(self):
        logger.info("Runner is stopped.")
        self.stop_run.set()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='Transfer files metadata to ASAPO')
    parser.add_argument('--config_path', type=str, help='Path to config directory')
    parser.add_argument(
        'identifier', type=str,
        help='Beamline and detector ID information separated by a single underscore')

    args = vars(parser.parse_args())
    config = construct_config(args.pop('config_path'), args.pop('identifier'))
    logging.basicConfig(format="%(asctime)s %(module)s %(lineno)-6d %(levelname)-6s %(message)s",
                        level=getattr(logging, config.log_level))
    logger.info("Start Asapo transfer with parameters %s", config)

    worker_args = dict(
        endpoint=config.endpoint,
        beamtime=config.beamtime,
        token=config.token,
        n_threads=config.n_threads,
        file_regex=config.file_regex,
        default_data_source=config.default_data_source,
        timeout=config.timeout,
        beamline=config.beamline,
        start_file_idx=config.start_file_idx)

    logger.info("Creating AsapoWorker with %s", worker_args)
    asapo_worker = AsapoWorker(**worker_args)
    run_transfer(asapo_worker, config, config.reconnect_timeout)


def verify_config(config):
    msg = ''
    if "endpoint" not in config:
        msg += "Parameter 'endpoint' is missing in config."

    par_types = [["endpoint", str],
                 ["token_path", str],
                 ["target_dir", str],
                 ["n_threads", int],
                 ["start_file_idx", int],
                 ["beamtime", str],
                 ["file_regex", str],
                 ["timeout", int],
                 ["reconnect_timeout", int],
                 ["log_level", str],
                 ["default_data_source", str]]

    for par_type in par_types:
        if par_type[0] in config and type(config[par_type[0]]) != par_type[1]:
            msg += "Parameter %s has wrong type." % par_type[0]

    for key in config:
        if key not in [par[0] for par in par_types]:
            msg += "Unexpected parameter %s." % key

    if "log_level" in config and config["log_level"] not in ["DEBUG", "INFO", "WARNING", "ERROR", "FATAL"]:
        msg += "Unexpected value of parameter 'log_level=%s'" % config["log_level"]

    if msg:
        msg = "Validation of config fails: " + msg
        logger.error(msg)
        raise ValueError(msg)


def construct_config(config_path, identifier):

    # parse identifier to extruct `beamline` and `detector_id`
    beamline, detector_id = identifier.split("_", maxsplit=1)

    # Read config file
    if config_path:
        with open("{}/asapo_transfer_{}.yaml".format(config_path, beamline), "r") as f:
            config = yaml.load(f.read(), Loader=yaml.FullLoader)
        verify_config(config)
    else:
        config = {}

    config['beamline'] = beamline
    config['detector_id'] = detector_id

    if 'default_data_source' not in config:
        config['default_data_source'] = detector_id
        # remove `.desy.de` if present at the end
        if detector_id[-8:] == ".desy.de":
            config['default_data_source'] = detector_id[:-8]

    token_path = config.pop('token_path', '/gpfs/asapo/shared/beamline_tokens')
    config['token'] = read_token(token_path, beamline)

    config['target_host'] = socket.getfqdn()
    config['signal_host'] = _constants.CONNECTION_LIST[beamline]['host']

    return TransferConfig(**config)


def run_transfer(asapo_worker, config, timeout=3, stop_event=None):
    asapo_transfer = None
    if stop_event is None:
        stop_event = Event()

        def signal_handler(s, f):
            stop_event.set()
            if asapo_transfer:
                asapo_transfer.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    while not stop_event.is_set():
        try:
            # The main reason for this loop is to find a free port. As using the
            # query object will raise if the port is already in use and
            # create_query chooses a random port each time, a free port should
            # be found eventually. Additionally, in case of an error from query,
            # the state of the query object is unclear. To be safe, always
            # create a new query object, independent of the exception.
            query = create_query(config.signal_host, config.detector_id)
            asapo_transfer = create_asapo_transfer(
                asapo_worker, query, config.target_host, config.target_dir,
                config.reconnect_timeout, stop_event)
            asapo_transfer.run()
        except Stopped:
            break
        except Exception:
            logger.warning(
                "Running Transfer stopped with an exception", exc_info=True)
            sleep(timeout)
            logger.info("Retrying connection")


if __name__ == "__main__":
    main()
