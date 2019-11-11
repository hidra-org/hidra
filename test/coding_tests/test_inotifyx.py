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


from __future__ import print_function

import os
import sys

import inotifyx


if __name__ == '__main__':

    if len(sys.argv) == 1:
        print('usage: inotify path [path ...]', file=sys.stderr)
        sys.exit(1)

    paths = sys.argv[1:]
    print("paths: ", paths)

    fd = inotifyx.init()

    wd_to_path = {}

    try:
        for path in paths:
            wd = inotifyx.add_watch(fd, path)
            wd_to_path[wd] = path

        print("wd_to_path: ", wd_to_path)

        try:
            while True:
                events = inotifyx.get_events(fd)
                for event in events:
                    path = wd_to_path[event.wd]
                    parts = [event.get_mask_description()]
                    a = event.get_mask_description()
                    a_array = a.split("|")
                    if event.name:
                        parts.append(event.name)

#                    print('%s: %s' % (path, ' '.join(parts)))
                    is_dir = ("IN_ISDIR" in a_array)
                    is_closed = ("IN_CLOSE" in a_array
                                 or "IN_CLOSE_WRITE" in a_array)
                    is_created = ("IN_CREATE" in a_array)

                    # if a new directory is created inside the monitored one,
                    # this one has to be monitored as well
                    if is_created and is_dir and event.name:
                        dirname = path + os.sep + event.name
                        if dirname in paths:
                            print("already contained in path list")
                        else:
                            wd = inotifyx.add_watch(fd, dirname)
                            wd_to_path[wd] = dirname
                            print("added path to watch", wd_to_path)

                    # only closed files are send
                    if is_closed and not is_dir:
                        parentDir = path
                        relativePath = ""
                        while True:
                            if parentDir not in paths:
                                (parentDir, relDir) = os.path.split(parentDir)
                                relativePath += os.sep + relDir
                            else:
                                print("parent", parentDir)
                                print("relativePath", relativePath)
                                print("filename", event.name)
                                break

        except KeyboardInterrupt:
            pass
    finally:
        os.close(fd)
