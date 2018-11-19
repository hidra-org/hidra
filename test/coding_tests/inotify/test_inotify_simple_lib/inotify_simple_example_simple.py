from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import sys

from inotify_simple import INotify, flags


def _main():
    watch_dir = "/tmp/watch_tree"

    try:
        for path in watch_dir:
            os.mkdir(path)
    except OSError:
        pass


    inotify = INotify()
    watch_flags = flags.CREATE | flags.DELETE | flags.MODIFY | flags.DELETE_SELF

    wd_to_path = {}

    wd = inotify.add_watch(watch_dir, watch_flags)
    wd_to_path[wd] = watch_dir
    print("wd_to_path: ", wd_to_path)

    with open(os.path.join(watch_dir, "test_file"), "w"):
        pass

    while True:
        for event in inotify.read():
            path = wd_to_path[event.wd]
            event_type = flags.from_mask(event.mask)

            print("PATH=[{}] FILENAME=[{}] EVENT_TYPES={}"
                  .format(path, event.name, event_type))

if __name__ == '__main__':
    _main()
