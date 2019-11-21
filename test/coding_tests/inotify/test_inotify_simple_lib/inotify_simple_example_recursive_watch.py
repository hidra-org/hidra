from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import shutil
import threading
import time

from inotify_simple import INotify, flags


def create_test_files(watch_dir):
    time.sleep(1)

    with open(os.path.join(watch_dir, "test_file"), "w"):
        pass

    test_dir = os.path.join(watch_dir, "test_dir")
    try:
        os.mkdir(test_dir)
    except OSError:
        pass

    with open(os.path.join(test_dir, "test_file2"), "w"):
        pass

    shutil.rmtree(test_dir)


def _main():
    watch_dir = "/tmp/watch_tree"
    create_thread = threading.Thread(target=create_test_files,
                                     args=(watch_dir,))

    try:
        os.mkdir(watch_dir)
    except OSError:
        pass

    inotify = INotify()
    watch_flags = (flags.CREATE | flags.DELETE | flags.MODIFY
                   | flags.DELETE_SELF | flags.ISDIR)

    wd_to_path = {}

    wd = inotify.add_watch(watch_dir, watch_flags)
    wd_to_path[wd] = watch_dir
    print("wd_to_path: ", wd_to_path)

    create_thread.start()

    while True:
        for event in inotify.read():
            path = wd_to_path[event.wd]
            event_type = flags.from_mask(event.mask)

            # remember dir name
            if flags.ISDIR in event_type and flags.CREATE in event_type:
                new_dir = os.path.join(path, event.name)
                wd = inotify.add_watch(new_dir, watch_flags)
                wd_to_path[wd] = new_dir

            print("PATH=[{}] FILENAME=[{}] EVENT_TYPES={}"
                  .format(path, event.name, event_type))


if __name__ == '__main__':
    _main()
