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
API_PATH = os.path.join(BASE_PATH, "src", "APIs")
print(API_PATH)

if SHARED_PATH not in sys.path:
    sys.path.insert(0, SHARED_PATH)

try:
    # search in global python modules first
    from hidra import Transfer  # noqa F401
except:
    # then search in local modules
    if API_PATH not in sys.path:
        sys.path.insert(0, API_PATH)
