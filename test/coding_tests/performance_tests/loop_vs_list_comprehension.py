from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import time
import os


def loop(inlist):
    result1 = []
    result2 = []

    for event in inlist:
        res2 = [os.path.join(event["abs"], event["rel"]), event["name"]]

        result1.append(event)
        result2.append(res2)


def loop_with_if(inlist):
    result1 = []
    result2 = []

    for event in inlist:
        res2 = [os.path.join(event["abs"], event["rel"]), event["name"]]

        if res2 not in result2:
            result1.append(event)
            result2.append(res2)

def list_comprehension(inlist):
    result1 = [event for event in inlist]
    result2 = [[os.path.join(event["abs"], event["rel"]), event["name"]]
               for event in inlist]

def list_comprehension_with_if(inlist):
    result2 = []

    result1 = [event for event in inlist
               if [os.path.join(event["abs"], event["rel"]),
                   event["name"]] not in result2]
    result2 = [[os.path.join(event["abs"], event["rel"]), event["name"]]
               for event in inlist]


if __name__ == "__main__":
    n_iter = 100000
    print("Testing for list of length", n_iter)

    # preparation
    inlist = []
    for i in range(n_iter):
        temp = {
            "abs": "abs",
            "rel": "rel",
            "name": "file{}".format(i)
        }
        inlist.append(temp)
    print("preparation finished")

    t = time.time()
    loop(inlist)
    print("using loop:", time.time() - t)

    t = time.time()
    list_comprehension(inlist)
    print("using list comprehension:", time.time() - t)

    t = time.time()
    loop_with_if(inlist)
    print("using loop with if:", time.time() - t)

    t = time.time()
    list_comprehension_with_if(inlist)
    print("using list comprehension with if:", time.time() - t)

# result
# Testing for list of length 100000
# preparation finished
# using loop: 0.138750076294
# using list comprehension: 0.138786077499
# using loop with if: 220.534312963
# using list comprehension with if: 0.209473133087
