from __future__ import print_function

import re


def main():
    regex = [
        ".*",
        ".*[tif|cbf]$",
        ".*(tif|cbf)$",
        ".*[.]$",
        "m1_.*.nxs$",
        ".*(log|tif|cbf|nxs)$",
        ".*m2.*nxs$"
    ]
    strings = [
        "m1_m123.nxs",
        "m2_m123.nxs",
        "m1_m123.cbf",
        "m1_m123.tif",
        "m1_m123.tfi",
        "abc.bin"
    ]

    for r in regex:
        print("regex: %s", r)

        pattern = re.compile(r)
        for s in strings:
            result = pattern.match(s)
            if result is not None:
                print("Match found: %s", s)
            else:
                print("Not matching: %s", s)
        print()


if __name__ == "__main__":
    main()
