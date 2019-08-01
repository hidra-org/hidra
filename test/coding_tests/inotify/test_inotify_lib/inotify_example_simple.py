import os
import shutil

import inotify.adapters


def _main():
    i = inotify.adapters.Inotify()

    watch_dir = "/tmp"
    i.add_watch(watch_dir)

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

    for event in i.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event

        print("PATH=[{}] FILENAME=[{}] EVENT_TYPES={}"
              .format(path, filename, type_names))


if __name__ == '__main__':
    _main()
