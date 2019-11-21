from __future__ import print_function

try:
    # The ConfigParser module has been renamed to configparser in Python 3
    from configparser import RawConfigParser
except ImportError:
    from ConfigParser import RawConfigParser

import os
import sys

from hidra.utils import parse_parameters

try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except NameError:
    CURRENT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))


def main():
    config_file = os.path.join(CURRENT_DIR, "test.conf")

    config = RawConfigParser()
    config.read(config_file)

    params = parse_parameters(config)
    print(params)


if __name__ == "__main__":
    main()
