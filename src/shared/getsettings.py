#!/usr/bin/env python

from __future__ import print_function
import __init__
import helpers
import argparse

def get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",
                        type=str,
                        default="/opt/hidra/conf/datamanager.conf",
                        help="Location of the configuration file")

    return parser.parse_args()


if __name__ == "__main__":

    args = get_arguments()

    config_file = args.config_file
    params = helpers.parse_parameters(helpers.read_config(config_file))["asection"]

    print("Configured settings:")
    print("Monitored direcory:            {}".format(params["monitored_dir"]))
    print("Watched subdirectories are:    {}".format(params["fix_subdirs"]))
    if params["store_data"]:
        print("Data is written to:            {}".format(params["local_target"]))
    else:
        print("Data is written to:            Data is not stored locally")
    if params["use_data_stream"]:
        print("Data is sent to:               {}".format(params["data_stream_targets"]))
    else:
        print("Data is sent to:               Data is not sent as priority stream anywhere")
    print("Remove data from the detector: {}".format(params["remove_data"]))
    print("Whitelist:                     {}".format(params["whitelist"]))
