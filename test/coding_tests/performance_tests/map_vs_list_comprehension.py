from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import time
import os

map_func=lambda x: x[0] == compare_value

def test_map(n_iter, compare_value, inlist):
    for i in range(n_iter):
        res = [list(map(map_func, inset))
               for inset in inlist]
#        print(res)


def list_comprehension(n_iter, compare_value, inlist):
    for i in range(n_iter):
        res = [[i[0] == compare_value for i in inset]
               for inset in inlist]
#        print(res)


if __name__ == "__main__":
    # preparation
#    n_iter = 1
    n_iter = 10000000
    compare_value = "test"
    inlist = [[["test", "a", "b"], ["test2", "a", "b"]],
              [["blubb1", "c", "d"], ["test", "c", "d"]]]

    print("Testing for list of lengh", n_iter)

    t = time.time()
    test_map(n_iter, compare_value, inlist)
    print("using map:", time.time() - t)

    t = time.time()
    list_comprehension(n_iter, compare_value, inlist)
    print("using list comprehension:", time.time() - t)

# result

# python2
# Testing for list of lengh 10000000
# using map: 11.5279810429
# using list comprehension: 4.77716088295

# python3
# Testing for list of lengh 10000000
# using map: 13.617951154708862
# using list comprehension: 8.376354932785034
