import os
import sys

# path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except:
    CURRENT_DIR = os.path.dirname(os.path.realpath('__file__'))
# CURRENT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
# CURRENT_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))

BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
SHARED_DIR = os.path.join(BASE_DIR, "src", "shared")
API_DIR = os.path.join(BASE_DIR, "src", "APIs")
CONFIG_DIR = os.path.join(BASE_DIR, "conf")

if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)

try:
    # search in local modules
    if API_DIR not in sys.path:
        sys.path.insert(0, API_DIR)
except ImportError:
    # search in global python modules
    from hidra import Transfer  # noqa F401
