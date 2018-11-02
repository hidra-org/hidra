import ConfigParser
import os
import sys

try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except:
    CURRENT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))
SHARED_DIR = os.path.join(BASE_DIR, "src", "shared")

if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)
del SHARED_DIR

from parameter_utils import parse_parameters

config_file = os.path.join(CURRENT_DIR, "test.conf")

config = ConfigParser.RawConfigParser()
config.read(config_file)

params = parse_parameters(config)

print params
