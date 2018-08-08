"""Package providing the event detector test classes.
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
SENDER_DIR = os.path.join(BASE_DIR, "src", "sender")

if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)

if SENDER_DIR not in sys.path:
    sys.path.insert(0, SENDER_DIR)
