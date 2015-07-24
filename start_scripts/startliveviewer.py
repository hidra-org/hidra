import os
import sys

import time
from threading import Thread

### Get configured paths and files
LIVEVIEWER_PATH = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) )
sys.path.append ( LIVEVIEWER_PATH )

from LiveViewer import LiveView

lv = LiveView()

lv.start()

time.sleep(100)
lv.stop()

