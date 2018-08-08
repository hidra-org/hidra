import re

regex = [".*", ".*[tif|cbf]$", ".*(tif|cbf)$", ".*[.]$", "m1_.*.nxs$", ".*(log|tif|cbf|nxs)$", ".*m2.*nxs$"]
strings = ["m1_m123.nxs", "m2_m123.nxs", "m1_m123.cbf", "m1_m123.tif", "m1_m123.tfi", "abc.bin"]

for r in regex:
    print "regex: {0}".format(r)

    pattern = re.compile(r)
    for s in strings:
        result = pattern.match(s)
        if result is not None:
            print "Match found: {0}".format(s)
        else:
            print "Not matching: {0}".format(s)
    print
