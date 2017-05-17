#!/usr/bin/env python

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

        targets = [socket.gethostname(), "50101", 1]

        # create HiDRA Transfer instance which wants data by request only
        self.query = Transfer("QUERY_NEXT", signal_host)
        self.query.initiate(targets)
        self.query.start()

        self.metadata = None
        self.data = None

    # helpers
    # ==================

    def __get_logger(self):
        # create the default logger used by the logging mixin
        log = logging.getLogger("fuse.log-mixin")
        log.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # add the handlers to the logger
        log.addHandler(ch)

        return log

    # Filesystem methods
    # ==================

    """
    def access(self, path, mode):
        pass

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    """
    def getattr(self, path, fh=None):
        self.log.debug("path={0}".format(path))

        if path == "/" or path.startswith("/.Trash"):
            st = os.lstat(path)
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

    """
    # The method readlink() returns a string representing the path to which the
    # symbolic link points. It may return an absolute or relative pathname.
    def readlink(self, path):
        pass

    # The method mknod() creates a filesystem node (file, device special file
    # or named pipe) named filename.
    def mknod(self, path, mode, dev):
        pass

    def rmdir(self, path):
        pass

    def mkdir(self, path, mode):
        pass

    # The method statvfs() perform a statvfs system call on the given path.
    def statfs(self, path):
        pass

    # The method unlink() removes (deletes) the file path. If the path is a
    # directory, OSError is raised.
    def unlink(self, path):
        pass

    # The method symlink() creates a symbolic link dst pointing to src.
    def symlink(self, name, target):
        pass

    def rename(self, old, new):
        pass

    def link(self, target, name):
        signal_host = "zitpcx19282.desy.de"
        targets = ["zitpcx19282.desy.de", "50101", 1]
        pass

    # The method utime() sets the access and modified times of the file
    # specified by path.
    def utimens(self, path, times=None):
        pass
    """

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

    """
    def create(self, path, mode, fi=None):
        pass
    """

    def read(self, path, length, offset, fh):
        # self.log.debug("read")

        self.read_pointer += length
        return self.data[self.read_pointer - length:self.read_pointer]

    """
    def write(self, path, buf, offset, fh):
        pass
    """

    # The method truncate() truncates the file's size. The file is truncated to
    # (at most) that size of the argument length
    def truncate(self, path, length, fh=None):
        self.log.debug("truncate")
        pass

    """
    # The method fsync() forces write of file with file descriptor fd to disk.
    def flush(self, path, fh):
        self.release(path, fh)
    """

    def release(self, path, fh):
        # self.log.debug("release")
        self.metadata = None
        self.data = None

    """
    def fsync(self, path, fdatasync, fh):
        self.release(path, fh)
    """

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("--signal_host",
                        type=str,
                        help="Host where HiDRA is runnning",
                        default=socket.gethostname())
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
