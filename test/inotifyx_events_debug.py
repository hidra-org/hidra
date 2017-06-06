from __future__ import print_function
from __future__ import unicode_literals

import os
from inotifyx import binding
from inotifyx.distinfo import version as __version__
# import time

print("inotifyx_version", __version__)

BASE_PATH = "/home/kuhnm/projects/hidra"
print("BASE_PATH", BASE_PATH)

constants = {}

for name in dir(binding):
    if name.startswith('IN_'):
        globals()[name] = constants[name] = getattr(binding, name)


# Source: inotifyx library code example
# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
class InotifyEvent (object):
    '''
    InotifyEvent(wd, mask, cookie, name)

    A representation of the inotify_event structure.  See the inotify
    documentation for a description of these fields.
    '''

    wd = None
    mask = None
    cookie = None
    name = None

    def __init__(self, wd, mask, cookie, name):
        self.wd = wd
        self.mask = mask
        self.cookie = cookie
        self.name = name

    def __str__(self):
        return '%s: %s' % (self.wd, self.get_mask_description())

    def __repr__(self):
        return '%s(%s, %s, %s, %s)' % (
            self.__class__.__name__,
            repr(self.wd),
            repr(self.mask),
            repr(self.cookie),
            repr(self.name),
        )

    def get_mask_description(self):
        '''
        Return an ASCII string describing the mask field in terms of
        bitwise-or'd IN_* constants, or 0.  The result is valid Python code
        that could be eval'd to get the value of the mask field.  In other
        words, for a given event:

        >>> from inotifyx import *
        >>> assert (event.mask == eval(event.get_mask_description()))
        '''

        parts = []
        for name, value in constants.items():
            if self.mask & value:
                parts.append(name)
        if parts:
            return parts
        return []


class EventDetector():

    def __init__(self):
        global BASE_PATH

        self.wd_to_path = {}
        self.fd = binding.init()

        self.timeout = 1

        path = BASE_PATH + "/data/source/local"
        wd = binding.add_watch(self.fd, path)
        self.wd_to_path[wd] = path

    def get_new_event(self):

        event_message_list = []

        events = [
            InotifyEvent(wd, mask, cookie, name)
            for wd, mask, cookie, name in binding.get_events(self.fd,
                                                             self.timeout)
        ]

        for event in events:

            if not event.name:
                continue

            path = self.wd_to_path[event.wd]

            parts_array = event.get_mask_description()

            if ("IN_ISDIR" not in parts_array
                    and "IN_CLOSE_WRITE" in parts_array
                    and [path, event.name]):

                event_message = {
                    "source_path": "parent_dir",
                    "relative_path": "rel_dir",
                    "filename": event.name
                }
                event_message_list.append(event_message)

        return event_message_list

    def stop(self):
        try:
            for wd in self.wd_to_path:
                binding.rm_watch(self.fd, wd)
        finally:
            os.close(self.fd)


if __name__ == '__main__':
    from subprocess import call
#    from __init__ import BASE_PATH
    import setproctitle
    import resource
#    import gc
#    # don't care about stuff that would be garbage collected properly
#    gc.collect()
#    import objgraph
#    from guppy import hpy

    setproctitle.setproctitle("inotifyx_events")

    source_file = os.path.join(BASE_PATH, "test_1024B.file")
    target_file_base = os.path.join(
        BASE_PATH, "data", "source", "local") + os.sep

    if not os.path.isdir(target_file_base):
        os.mkdir(target_file_base)

    eventdetector = EventDetector()

    min_loop = 100
    max_loop = 20000
    steps = 10
    step_loop = max_loop / steps
    print("Used steps:", steps)

    memory_usage_old = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print("Memory usage at start: {0} (kb)".format(memory_usage_old))

#    hp = hpy()
#    hp.setrelheap()
    try:
        for s in range(steps):
            start = min_loop + s * step_loop
            stop = start + step_loop
#            print ("start=", start, "stop=", stop)
            for i in range(start, stop):

                target_file = "{0}{1}.cbf".format(target_file_base, i)
                call(["cp", source_file, target_file])

                if i % 100 == 0:
                    event_list = eventdetector.get_new_event()
#                    if event_list:
#                        print("event_list:", event_list)

#                time.sleep(1)
            memory_usage_new = (
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            print("Memory usage in iteration {0}: {1} (kb)"
                  .format(s, memory_usage_new))
            if memory_usage_new > memory_usage_old and s != 0:
                memory_usage_old = memory_usage_new
                # objgraph.show_most_common_types()
#                print(hp.heap())

    except KeyboardInterrupt:
        pass
    finally:
        for number in range(min_loop, stop):
            try:
                target_file = "{0}{1}.cbf".format(target_file_base, number)
                os.remove(target_file)
            except OSError:
                pass
