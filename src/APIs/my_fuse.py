#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import stat

from fuse import FUSE, FuseOSError, Operations

from hidra import Transfer

class Passthrough(Operations):
    def __init__(self):
        signal_host = "zitpcx19282.desy.de"
        targets = ["zitpcx19282.desy.de", "50101", 1]
        # create HiDRA Transfer instance which wants data by request only
        self.query = Transfer("QUERY_NEXT", signal_host)
        self.query.initiate(targets)
        self.query.start()

        self.metadata = None
        self.data = None

    # Filesystem methods
    # ==================

    """
    def access(self, path, mode):
        pass

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def getattr(self, path, fh=None):
        if self.metadata is None and self.data is None:
            [self.metadata, self.data] = self.query.get()

        return {
            "st_mode": (stat.S_IFREG | 0644),
            "st_nlink": 1,
            "st_ctime": self.metadata["file_create_time"],
            "st_mtime": self.metadata["file_mod_time"],
            "st_size": self.metadata["filesize"]
        }

    def readdir(self, path, fh):
#        if self.metadata is None and self.data is None:
#            [self.metadata, self.data] = self.query.get()
#        return [".", "..", self.data["filename"]]
#        return [".", "..", "next_file"]

    # The method readlink() returns a string representing the path to which the symbolic link points. It may return an absolute or relative pathname.
    def readlink(self, path):
        pass

    # The method mknod() creates a filesystem node (file, device special file or named pipe) named filename.
    def mknod(self, path, mode, dev):
        pass

    def rmdir(self, path):
        pass

    def mkdir(self, path, mode):
        pass

    # The method statvfs() perform a statvfs system call on the given path.
    def statfs(self, path):
        pass

    # The method unlink() removes (deletes) the file path. If the path is a directory, OSError is raised.
    def unlink(self, path):
        pass

    # The method symlink() creates a symbolic link dst pointing to src.
    def symlink(self, name, target):
        pass

    def rename(self, old, new):
        pass

    def link(self, target, name):
        pass

    # The method utime() sets the access and modified times of the file specified by path.
    def utimens(self, path, times=None):
        pass
    """

    # File methods
    # ============

    def open(self, path, flags):
        if self.metadata is None and self.data is None:
            [self.metadata, self.data] = self.query.get()

    """
    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
    """

    def read(self, path, length, offset, fh):
        if self.data is None:
            [self.metadata, self.data] = self.query.get()

        return self.data

    """
    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)
    """

    def truncate(self, path, length, fh=None):
        pass

    """
    # The method fsync() forces write of file with file descriptor fd to disk.
    def flush(self, path, fh):
        self.release(path, fh)
    """

    def release(self, path, fh):
        self.metadata = None
        self.data = None

    """
    def fsync(self, path, fdatasync, fh):
        self.release(path, fh)
    """

def main(mountpoint):
    FUSE(Passthrough(), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1])
