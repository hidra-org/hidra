import os
import sys

try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except:
    CURRENT_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))
#    CURRENT_DIR = os.path.dirname(os.path.realpath('__file__'))

BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
API_DIR = os.path.join(BASE_DIR, "src", "APIs")
SHARED_DIR = os.path.join(BASE_DIR, "src", "shared")

if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)

try:
    # search in global python modules first
    from hidra import Transfer  # noqa F401
except:
    # then search in local modules
    if API_DIR not in sys.path:
        sys.path.insert(0, API_DIR)
