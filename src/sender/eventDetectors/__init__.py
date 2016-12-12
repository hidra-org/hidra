import os
import sys

try:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.realpath(__file__)))))
except:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.realpath(sys.argv[0])))))

SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)
