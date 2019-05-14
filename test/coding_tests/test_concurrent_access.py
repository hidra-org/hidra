from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from multiprocessing import Process
import os
import time

class CheckModTime(Process):
    def __init__(self, files):
        Process.__init__(self)
        self.files = files

    def run(self):
        for i in range(1000000):
            for f in self.files:
                try:
                    os.stat(f).st_mtime
                except OSError:
                    return

if __name__ == "__main__":
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))

    data_dir = os.path.join(BASE_DIR, "data", "source")
    filename = os.path.join(data_dir, "file.test")

    # create empty file
    with open(filename, "w") as f:
        pass

    checker = CheckModTime([filename])
    checker.start()
    time.sleep(0.1)

    os.remove(filename)

    checker.join()

