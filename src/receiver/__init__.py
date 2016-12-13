import os
import sys

# path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
try:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.realpath(__file__))))
except:
    BASE_PATH = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.realpath('__file__'))))
#    BASE_PATH = os.path.dirname(
#        os.path.dirname(
#           os.path.dirname(
#               os.path.abspath(sys.argv[0]))))
#    BASE_PATH = os.path.dirname(
#        os.path.dirname(
#           os.path.dirname(
#               os.path.realpath(sys.argv[0])))))

SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")
API_PATH = os.path.join(BASE_PATH, "src", "APIs")
CONFIG_PATH = os.path.join(BASE_PATH, "conf")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)

try:
    # search in global python modules first
    from hidra import Transfer
except:
    # then search in local modules
    if API_PATH not in sys.path:
        sys.path.append(API_PATH)
