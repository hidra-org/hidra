from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import multiprocessing
import os
import select
import shutil
import sys
import threading
import time

from inotify_simple import INotify, flags


def create_test_files(watch_dir, n_files):
    t_start = time.time()
    for i in range(n_files):
        with open(os.path.join(watch_dir, "test_file"), "w") as f:
            pass

    t_needed = time.time() - t_start
    print("created {} in {} s, ({} Hz)".format(n_files, t_needed, n_files / t_needed))


def create_and_get_events(watch_dir, n_files, use_pr):
    if use_pr:
        print("use multiprocessing")
        job_type = multiprocessing.Process
    else:
        print("use threading")
        job_type = threading.Thread

    create_pr = job_type(
        target=create_test_files,
        args=(watch_dir, n_files)
    )

    try:
        os.mkdir(watch_dir)
    except OSError:
        pass

    inotify = INotify()
    watch_flags = flags.CREATE | flags.OPEN | flags.MODIFY

    wd_to_path = {}

    wd = inotify.add_watch(watch_dir, watch_flags)
    wd_to_path[wd] = watch_dir

    create_pr.start()

    n_events = 0
    timeout = 2 * 1000  # is in ms
    t = time.time()
    run_loop = True
    while run_loop:
        events = inotify.read(timeout=timeout)

        if not events:
            run_loop = False

        for event in events:
            if event.wd < 0:
                continue

            event_type = flags.from_mask(event.mask)

            if flags.OPEN in event_type:
                n_events += 1

    t_needed = time.time() - t
    print("n_events {} in {} s, ({} Hz)".format(n_events, t_needed, n_events / t_needed))
    create_pr.join()


def _main():
    watch_dir = "/tmp/watch_tree"
    n_files = 1000000

    use_pr = True
    create_and_get_events(watch_dir, n_files, use_pr)

    use_pr = False
    create_and_get_events(watch_dir, n_files, use_pr)


if __name__ == '__main__':
    _main()
