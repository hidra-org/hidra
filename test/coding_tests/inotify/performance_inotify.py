from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import shutil
import threading
import time

import inotify.adapters

def create_test_files(watch_dir, n_files):
    for i in range(n_files):
        with open(os.path.join(watch_dir, "test_file"), "w"):
            pass


def _main():
    watch_dir = "/tmp/watch_tree"
    n_files = 1000000
    #n_files = 1000000

    create_thread = threading.Thread(target=create_test_files,
                                 args=(watch_dir, n_files))

    try:
        os.mkdir(watch_dir)
    except OSError:
        pass

    i = inotify.adapters.InotifyTree(watch_dir)

    create_thread.start()

    n_events = 0
    t = time.time()
    for event in i.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event
        if "IN_OPEN" in type_names:
            n_events += 1

            if n_events % 100000 == 0:
                print(n_events)

            if n_events == n_files:
                break

    print("n_events", n_events, "total {}s".format(time.time() - t))


if __name__ == '__main__':
    _main()
