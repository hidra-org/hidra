# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import shutil
import sys
import threading
import time

import inotifyx


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

    fd = inotifyx.init()

    wd_to_path = {}

    try:
        wd = inotifyx.add_watch(fd, watch_dir)
        wd_to_path[wd] = watch_dir

        print("wd_to_path: ", wd_to_path)
    except Exception:
        print("stopped")
        os.close(fd)
        sys.exit(1)

    create_thread.start()

    try:
        while True:
            events = inotifyx.get_events(fd)

            for event in events:
                path = wd_to_path[event.wd]
                event_type = event.get_mask_description()
                event_type_array = event_type.split("|")

                # remember dir name
                if "IN_ISDIR" in event_type_array and "IN_CREATE" in event_type:
                    new_dir = os.path.join(path, event.name)
                    wd = inotifyx.add_watch(fd, new_dir)
                    wd_to_path[wd] = new_dir

                print("PATH=[{}] FILENAME=[{}] EVENT_TYPES={}"
                      .format(path, event.name, event_type_array))

    except KeyboardInterrupt:
        pass
    finally:
        os.close(fd)


if __name__ == '__main__':
    _main()
