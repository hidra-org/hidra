#!/usr/bin/env python

#http://stackoverflow.com/questions/168409/how-do-you-get-a-directory-listing-sorted-by-creation-date-in-python

from stat import S_ISREG, ST_MTIME, ST_MODE
import os, sys, time
import bisect

RING_BUFFER_SIZE = 5

# path to the directory (relative or absolute)
dirpath = "/opt/hidra/target/local"

# get all entries in the directory
entries = (os.path.join(dirpath, fn) for fn in os.listdir(dirpath))
# get the corresponding stats
entries = ((os.stat(path), path) for path in entries)

# leave only regular files, insert modification date
entries = [[stat[ST_MTIME], path]
           for stat, path in entries if S_ISREG(stat[ST_MODE])]

entries = sorted(entries, reverse=True)
len_entries = len(entries)
print entries

#print entries
#print len_entries

#targetFilepath = "/opt/hidra/test.tif"
#entries[:0] = [[os.stat(path)[ST_MTIME], targetFilepath]]
#print "after prepend"
#print entries

filename    = "/opt/hidra/test.tif"
#fileModTime = os.stat(filename)[ST_MTIME]
fileModTime = 1436956680

entries[:0] = [[fileModTime, filename]]
print "after insort"
print entries

entries = sorted(entries, reverse=True)
print "sort again"
print entries


if len_entries > RING_BUFFER_SIZE:
    print "files to remove"
    for mod_time, path in entries[RING_BUFFER_SIZE:]:
        print mod_time, path
        pass
#        os.remove(path)
#        entries.remove([mod_time, path])


print
print "content"
for cdate, path in entries:
    print time.ctime(cdate), os.path.basename(path)
