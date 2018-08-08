"""Package providing the data fetcher test classes.
"""

import os
import sys

CURRENT_DIR = os.path.realpath(__file__)

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(CURRENT_DIR)
        )
    )
)

SHARED_DIR = os.path.join(BASE_DIR, "src", "shared")
DATAFETCHER_DIR = os.path.join(BASE_DIR, "src", "sender", "datafetchers")
API_DIR = os.path.join(BASE_DIR, "src", "APIs")

if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)

if DATAFETCHER_DIR not in sys.path:
    sys.path.insert(0, DATAFETCHER_DIR)

try:
    # search in global python modules first
    from hidra import Transfer  # noqa F401 # pylint: disable=import-error
except ImportError:
    # then search in local modules
    if API_DIR not in sys.path:
        sys.path.insert(0, API_DIR)
