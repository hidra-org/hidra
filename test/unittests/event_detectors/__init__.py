from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import os
import sys

try:
    CURRENT_DIR = os.path.realpath(__file__)
except:
    CURRENT_DIR = os.path.realpath(sys.argv[0])

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(CURRENT_DIR)
        )
    )
)

SHARED_DIR = os.path.join(BASE_DIR, "src", "shared")
EVENT_DETECTOR_DIR = os.path.join(BASE_DIR, "src", "sender", "eventdetectors")
API_DIR = os.path.join(BASE_DIR, "src", "APIs")

if SHARED_DIR not in sys.path:
    sys.path.append(SHARED_DIR)

if EVENT_DETECTOR_DIR not in sys.path:
    sys.path.append(EVENT_DETECTOR_DIR)

# TODO hack to work around already installed hidra
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

try:
    # search in global python modules first
    from hidra import Transfer  # noqa F401
except:
    # then search in local modules
    if API_DIR not in sys.path:
        sys.path.insert(0, API_DIR)
