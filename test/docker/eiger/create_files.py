from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import hashlib
import os
import tempfile


def get_hash(filename):
    with open(filename, "rb") as f:
        data = f.read()
        md5sum = hashlib.md5()
        md5sum.update(data)
        return md5sum.hexdigest()


def create_file(filename, size=1000000):
    # write to a temporary file so that the server never sees incomplete files
    # don't use /tmp because /webcontent is a volume and rename is not possible
    # across file systems
    os.makedirs("/webcontent/tmp", exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(dir="/webcontent/tmp")
    with os.fdopen(tmp_fd, "wb") as f:
        f.write(os.urandom(size))

    file_hash = get_hash(tmp_name)

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    os.rename(tmp_name, filename)

    return file_hash


def create_files(n, size, path, prefix, ext="h5"):
    parent = "/webcontent/data"
    filenames = [
        os.path.join(path, prefix + str(i) + "." + ext)
        for i in range(n)]

    hashes = {
        name: create_file(os.path.join(parent, name), size)
        for name in filenames}

    return hashes


def main():
    """Simulate data taking.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--number",
                        type=int,
                        help="Number of files",
                        default=1)
    parser.add_argument("--size",
                        type=int,
                        help="Size of the files",
                        default=1000000)
    parser.add_argument("--path",
                        type=str,
                        help="File path",
                        default="current/raw")
    parser.add_argument("--prefix",
                        type=str,
                        help="File name prefix",
                        default="test_")
    parser.add_argument("--ext",
                        type=str,
                        help="File name extension",
                        default="h5")

    options = parser.parse_args()

    hashes = create_files(
        n=options.number,
        size=options.size,
        path=options.path,
        prefix=options.prefix,
        ext=options.ext)

    for key, value in hashes.items():
        print("{}: {}".format(key, value))


if __name__ == "__main__":
    main()
