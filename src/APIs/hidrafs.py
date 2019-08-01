#!/usr/bin/env python

# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""
This module provides a fuseFS interface to hidra.
"""

# pylint: disable=missing-docstring
# pylint: disable=unused-argument
# pylint: disable=no-self-use

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import os
import stat
import logging
import argparse
import socket
import setproctitle

from fuse import FUSE, Operations

from hidra import Transfer


class Passthrough(Operations):

    def __init__(self, signal_host):

        self.log = self.__get_logger()

        self.timeout = 2000
        self.read_pointer = 0

        targets = [socket.getfqdn(), "50101", 1]

        # create HiDRA Transfer instance which wants data by request only
        self.query = Transfer("QUERY_NEXT", signal_host)
        self.query.initiate(targets)
        self.query.start()

        self.metadata = None
        self.data = None

    # utils
    # ==================

    def __get_logger(self):
        # create the default logger used by the logging mixin
        log = logging.getLogger("fuse.log-mixin")
        log.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        # add the handlers to the logger
        log.addHandler(handler)

        return log

    # Filesystem methods
    # ==================

#    def access(self, path, mode):
#        pass

#    def chmod(self, path, mode):
#        pass

#    def chown(self, path, uid, gid):
#        pass

    def getattr(self, path, fh=None):
        self.log.debug("path=%s", path)

        if path == "/" or path.startswith("/.Trash"):
            st = os.lstat(path)  # pylint: disable=invalid-name
            return {
                "st_mode": getattr(st, "st_mode"),
                "st_nlink": getattr(st, "st_nlink"),
                "st_uid": getattr(st, "st_uid"),
                "st_gid": getattr(st, "st_gid"),
                "st_ctime": getattr(st, "st_ctime"),
                "st_mtime": getattr(st, "st_mtime"),
                "st_size": getattr(st, "st_size")
            }
        else:
            if self.metadata is None and self.data is None:
                self.log.debug("get")
                [self.metadata, self.data] = self.query.get(self.timeout)

            return {
                "st_mode": (stat.S_IFREG | 0644),
                "st_nlink": 1,
                "st_uid": 1000,
                "st_gid": 1000,
                "st_ctime": self.metadata["file_create_time"],
                "st_mtime": self.metadata["file_mod_time"],
                "st_size": self.metadata["filesize"]
            }

    def readdir(self, path, fh):
        # if self.metadata is None and self.data is None:
        [self.metadata, self.data] = self.query.get(self.timeout)

        if self.metadata is None:
            return [".", ".."]
        else:
            return [".", "..", self.metadata["filename"]]

    # The method readlink() returns a string representing the path to which the
    # symbolic link points. It may return an absolute or relative pathname.
#    def readlink(self, path):
#        pass

    # The method mknod() creates a filesystem node (file, device special file
    # or named pipe) named filename.
#    def mknod(self, path, mode, dev):
#        pass

#    def rmdir(self, path):
#        pass

#    def mkdir(self, path, mode):
#        pass

    # The method statvfs() perform a statvfs system call on the given path.
#    def statfs(self, path):
#        pass

    # The method unlink() removes (deletes) the file path. If the path is a
    # directory, OSError is raised.
#    def unlink(self, path):
#        pass

    # The method symlink() creates a symbolic link dst pointing to src.
#    def symlink(self, name, target):
#        pass

#    def rename(self, old, new):
#        pass

#    def link(self, target, name):
#        signal_host = "zitpcx19282.desy.de"
#        targets = ["zitpcx19282.desy.de", "50101", 1]
#        pass

    # The method utime() sets the access and modified times of the file
    # specified by path.
#    def utimens(self, path, times=None):
#        pass

    # File methods
    # ============

    # The method open() opens the file file and set various flags according to
    # flags and possibly its mode according to mode.The default mode is 0777
    # (octal), and the current umask value is first masked out.
    def open(self, path, flags):
        # self.log.debug("open")
        if self.metadata is None and self.data is None:
            self.log.debug("get")
            [self.metadata, self.data] = self.query.get(self.timeout)
        # for reading
        self.read_pointer = 0
        return 0

#    def create(self, path, mode, fi=None):
#        pass

    def read(self, path, length, offset, fh):
        # self.log.debug("read")

        self.read_pointer += length
        return self.data[self.read_pointer - length:self.read_pointer]

#    def write(self, path, buf, offset, fh):
#        pass

    # The method truncate() truncates the file's size. The file is truncated to
    # (at most) that size of the argument length
    def truncate(self, path, length, fh=None):
        self.log.debug("truncate")

    # The method fsync() forces write of file with file descriptor fd to disk.
#    def flush(self, path, fh):
#        self.release(path, fh)

    def release(self, path, fh):
        # self.log.debug("release")
        self.metadata = None
        self.data = None

#    def fsync(self, path, fdatasync, fh):
#        self.release(path, fh)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is runnning",
                        default=socket.getfqdn())
    parser.add_argument("--mount",
                        type=str,
                        help="Mount point under which hidrafs should be"
                             "mounted")
    parser.add_argument("--procname",
                        type=str,
                        help="Name with which the service should be running",
                        default="hidrafs")

    arguments = parser.parse_args()

    setproctitle.setproctitle(arguments.procname)

    FUSE(Passthrough(arguments.signal_host), arguments.mount,
         nothreads=True, foreground=True)


if __name__ == '__main__':
    main()
