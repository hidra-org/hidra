import os
import sys

import time
from threading import Thread

### Get configured paths and files
LIVEVIEWER_PATH = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) + os.sep + "src"
print LIVEVIEWER_PATH
sys.path.append ( LIVEVIEWER_PATH )

from LiveViewer import LiveView

lv = LiveView()

lv.start()

time.sleep(100)
lv.stop()

