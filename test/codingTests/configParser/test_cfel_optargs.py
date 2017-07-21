import ConfigParser
import os
import sys

try:
    CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
except:
    CURRENT_PATH = os.path.dirname(os.path.abspath(sys.argv[0]))

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_PATH)))
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if SHARED_PATH not in sys.path:
    sys.path.insert(0, SHARED_PATH)
del SHARED_PATH

from cfel_optarg import parse_parameters


config_file = "/opt/hidra/conf/test.conf"

config = ConfigParser.RawConfigParser()
config.read(config_file)

params = parse_parameters(config)

print params
