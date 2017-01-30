import os
import sys

# path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
try:
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
except:
    CURRENT_DIR = os.path.dirname(os.path.realpath('__file__'))
#           os.path.dirname(
#               os.path.abspath(sys.argv[0]))))
#           os.path.dirname(
#               os.path.realpath(sys.argv[0])))))


BASE_PATH = os.path.dirname(os.path.dirname(CURRENT_DIR))
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")
EVENTDETECTOR_PATH = os.path.join(CURRENT_DIR, "eventdetectors")
DATAFETCHER_PATH = os.path.join(CURRENT_DIR, "datafetchers")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)

if EVENTDETECTOR_PATH not in sys.path:
    sys.path.append(EVENTDETECTOR_PATH)

if DATAFETCHER_PATH not in sys.path:
    sys.path.append(DATAFETCHER_PATH)
