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
import sys

import inotifyx


def _main():
    watch_dir = "/tmp/watch_tree"

    try:
        for path in watch_dir:
            os.mkdir(path)
    except OSError:
        pass

    fd = inotifyx.init()

    wd_to_path = {}

    try:
        wd = inotifyx.add_watch(fd, watch_dir)
        wd_to_path[wd] = watch_dir
        print("wd_to_path: ", wd_to_path)
    except Exception as excp:
        print("stopped")
        print("Exception was", excp)
        os.close(fd)
        sys.exit(1)

    with open(os.path.join(watch_dir, "test_file"), "w"):
        pass

    try:
        while True:
            events = inotifyx.get_events(fd)

            for event in events:
                path = wd_to_path[event.wd]
                event_type = event.get_mask_description()
                event_type_array = event_type.split("|")

                print("PATH=[{}] FILENAME=[{}] EVENT_TYPES={}"
                      .format(path, event.name, event_type_array))

    except KeyboardInterrupt:
        pass
    finally:
        os.close(fd)


if __name__ == '__main__':
    _main()
