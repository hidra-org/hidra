import copy
from pathlib import Path
import sys
import yaml
from unittest.mock import patch

from server import argument_parsing  # noqa


default_config = {
    'controlserver': {
        'backup_file': '/beamline/support/hidra/instances_{bl}.txt',
        'beamline': 'p00',
        'config_file': None,
        'hidra_config_name': 'datamanager_{bl}_{det}.yaml',
        'ldapuri': 'it-ldap-slave.desy.de:1389',
        'log_name': 'hidra-control-server_{bl}.log',
        'log_path': '/var/log/hidra',
        'log_size': 10485760,
        'netgroup_template': 'a3{bl}-hosts',
        'onscreen': False,
        'procname': 'hidra-control-server_{bl}',
        'verbose': False},
    'hidraconfig_static': {
        'datafetcher': {
            'chunksize': 10485760,
            'http_fetcher': {
                'fix_subdirs': [
                    'current/raw',
                    'current/scratch_bl',
                    'commissioning/raw',
                    'commissioning/scratch_bl',
                    'local']},
            'number_of_streams': 16,
            'type': 'http_fetcher',
            'use_data_stream': False},
        'eventdetector': {
            'http_events': {
                'fix_subdirs': [
                    'current/raw',
                    'current/scratch_bl',
                    'commissioning/raw',
                    'commissioning/scratch_bl',
                    'local']},
            'inotifyx_events': {
                'action_time': 150,
                'fix_subdirs': [
                    'current/raw',
                    'current/scratch_bl',
                    'commissioning/raw',
                    'commissioning/scratch_bl',
                    'local'],
                'time_till_closed': 2,
                'use_cleanup': False},
            'type': 'http_events'},
        'general': {
            'com_port': 'random',
            'log_path': '/var/log/hidra',
            'log_size': 10485760,
            'request_port': 'random',
            'taskprovider_timeout': None,
            'use_statserver': True}},
    'hidraconfig_variable': {
        'datafetcher': {
            'local_target': '/beamline/{bl}'},
        'general': {
            'log_name': 'datamanager_{bl}',
            'procname': 'hidra_{bl}',
            'username': '{bl}user'}}}


def test_default_config():
    with patch('sys.argv', ["server.py", "--beamline", "p00"]):
        config = argument_parsing()

    assert config == default_config


def test_specified_config(tmpdir):
    config_path = tmpdir / "control_server_p00.yaml"

    new_config = copy.deepcopy(default_config)

    new_config["hidraconfig_variable"]["general"]["username"] = "p00user"

    config_path.write_text(yaml.dump(new_config), 'utf8')

    # the returned config also contains the command line arguments, therefore
    # the config_file needs to be updated
    new_config["controlserver"]["config_file"] = str(config_path)

    with patch(
        'sys.argv', [
            "server.py", "--beamline", "p00",
            "--config_file", str(config_path)]):
        config = argument_parsing()

    assert config == new_config
