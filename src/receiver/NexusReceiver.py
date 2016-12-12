from __future__ import print_function

import sys
import argparse
import logging
import os
import json
import subprocess
import re
import errno
try:
    import ConfigParser
except:
    import configparser as ConfigParser


BASE_PATH = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__))))
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")
API_PATH = os.path.join(BASE_PATH, "src", "APIs")
CONFIG_PATH = os.path.join(BASE_PATH, "conf")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)
del SHARED_PATH
del BASE_PATH

import helpers

try:
    # search in global python modules first
    from hidra import Transfer
except:
    # then search in local modules
    if API_PATH not in sys.path:
        sys.path.append(API_PATH)
    del API_PATH

    from hidra import Transfer

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def argument_parsing():
    default_config = os.path.join(CONFIG_PATH, "nexusReiceiver.conf")

    ##################################
    #   Get command line arguments   #
    ##################################

    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",
                        type=str,
                        help="Location of the configuration file")

    parser.add_argument("--log_path",
                        type=str,
                        help="Path where logfile will be created")
    parser.add_argument("--log_name",
                        type=str,
                        help="Filename used for logging")
    parser.add_argument("--log_size",
                        type=int,
                        help="File size before rollover in B (linux only)")
    parser.add_argument("--verbose",
                        help="More verbose output",
                        action="store_true")
    parser.add_argument("--onscreen",
                        type=str,
                        help="Display logging on screen "
                             "(options are CRITICAL, ERROR, WARNING, "
                             "INFO, DEBUG)",
                        default=False)

    parser.add_argument("--whitelist",
                        type=str,
                        help="List of hosts allowed to connect")
    parser.add_argument("--target_dir",
                        type=str,
                        help="Where incoming data will be stored to")
    parser.add_argument("--data_stream_ip",
                        type=str,
                        help="Ip of dataStream-socket to pull new files from")
    parser.add_argument("--data_stream_port",
                        type=str,
                        help="Port number of dataStream-socket to pull new "
                             "files from")

    arguments = parser.parse_args()
    arguments.config_file = arguments.config_file \
        or default_config

    # check if config_file exist
    helpers.check_file_existance(arguments.config_file)

    ##################################
    # Get arguments from config file #
    ##################################

    config = ConfigParser.RawConfigParser()
    config.readfp(helpers.FakeSecHead(open(arguments.config_file)))

    arguments.log_path = arguments.log_path \
        or config.get('asection', 'log_path')
    arguments.log_name = arguments.log_name \
        or config.get('asection', 'log_name')

    if not helpers.is_windows():
        arguments.log_size = arguments.log_size \
            or config.get('asection', 'log_size')

    try:
        arguments.whitelist = arguments.whitelist \
            or json.loads(config.get('asection', 'whitelist'))
    except ValueError:
        ldap_cn = config.get('asection', 'whitelist')
        p = subprocess.Popen(
            ["ldapsearch",
             "-x",
             "-H ldap://it-ldap-slave.desy.de:1389",
             "cn=" + ldap_cn, "-LLL"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        lines = p.stdout.readlines()

        matchHost = re.compile(r'nisNetgroupTriple: [(]([\w|\S|.]+),.*,[)]',
                               re.M | re.I)
        arguments.whitelist = []

        for line in lines:

            if matchHost.match(line):
                if matchHost.match(line).group(1) not in arguments.whitelist:
                    arguments.whitelist.append(matchHost.match(line).group(1))
    except:
        arguments.whitelist = json.loads(
            config.get('asection', 'whitelist').replace("'", '"'))

    arguments.target_dir = arguments.target_dir \
        or config.get('asection', 'target_dir')

    arguments.data_stream_ip = arguments.data_stream_ip \
        or config.get('asection', 'data_stream_ip')
    arguments.data_stream_port = arguments.data_stream_port \
        or config.get('asection', 'data_stream_port')

    ##################################
    #     Check given arguments      #
    ##################################

    logfile = os.path.join(arguments.log_path, arguments.log_name)

    # enable logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handlers = helpers.get_log_handlers(logfile, arguments.log_size,
                                        arguments.verbose, arguments.onscreen)

    if type(handlers) == tuple:
        for h in handlers:
            root.addHandler(h)
    else:
        root.addHandler(handlers)

    # check target directory for existance
    helpers.check_dir_existance(arguments.target_dir)

    # check if logfile is writable
    helpers.check_log_file_writable(arguments.log_path,
                                    arguments.log_name)

    return arguments


class NexusReceiver:
    def __init__(self):
        self.transfer = None

        try:
            arguments = argument_parsing()
        except:
            self.log = self.get_logger()
            raise

        self.log = self.get_logger()

        self.whitelist = arguments.whitelist

        self.log.info("Configured whitelist: {0}".format(self.whitelist))

        self.target_dir = os.path.normpath(arguments.target_dir)
        self.data_ip = arguments.data_stream_ip
        self.data_port = arguments.data_stream_port

        self.log.info("Writing to directory '{0}'".format(self.target_dir))

        self.transfer = Transfer("nexus", use_log=True)

        try:
            self.run()
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping due to unknown error condition",
                           exc_info=True)
        finally:
            self.stop()

    def get_logger(self):
        logger = logging.getLogger("NexusReceiver")
        return logger

    def open_callback(self, params, filename):
        # TODO
        try:
            BASE_PATH = os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.realpath(__file__))))
        except:
            BASE_PATH = os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.abspath(sys.argv[0]))))
        print (BASE_PATH)

        target_file = os.path.join(
            BASE_PATH, "data", "target", "local", filename)

        try:
            params["target_fp"] = open(target_file, "wb")
        except IOError as e:
            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:
                try:
                    target_path = os.path.split(target_file)[0]
                    print ("target_path", target_path)
                    os.makedirs(target_path)

                    params["target_fp"] = open(target_file, "wb")
                    print ("New target directory created:", target_path)
                except:
                    raise
            else:
                    raise
        print (params, filename)

    def read_callback(self, params, received_data):
        metadata = received_data[0]
        data = received_data[1]
        print (params, metadata)

        params["target_fp"].write(data)

    def close_callback(self, params, data):
        print (params, data)
        params["target_fp"].close()

    def run(self):
        callback_params = {"target_fp": None}

        try:
            self.transfer.start([self.data_ip, self.data_port], self.whitelist)
#            self.transfer.start(self.data_port)
        except:
            self.log.error("Could not initiate stream", exc_info=True)
            raise

        # run loop, and wait for incoming messages
        while True:
            try:
                data = self.transfer.read(callback_params,
                                          self.open_callback,
                                          self.read_callback,
                                          self.close_callback)
                logging.debug("Retrieved: " + str(data)[:100])

#                if data == "CLOSE_FILE":
#                    break
            except KeyboardInterrupt:
                break
            except:
                self.log.error("Could not read")
                raise

    def stop(self):
        if self.transfer:
            self.log.info("Shutting down receiver...")
            self.transfer.stop()
            self.transfer = None

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == "__main__":
    # start file receiver
    receiver = NexusReceiver()
