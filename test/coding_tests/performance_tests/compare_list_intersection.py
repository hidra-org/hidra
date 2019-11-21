from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

from itertools import filterfalse
import time


def use_itertools(list_1, list_2, n_iter):
    # works in python3 only
    for i in range(n_iter):
        _ = list(filterfalse(set(list_2).__contains__, list_1))


def use_sets(list_1, list_2, n_iter):
    for i in range(n_iter):
        _ = list(set(list_1)-set(list_2))


def use_list_comprehension(list_1, list_2, n_iter):
    for i in range(n_iter):
        _ = [x for x in list_1 if x not in list_2]


def main():

    list_1 = range(100)
    list_2 = range(50, 100)

    n_iter = 10000000

#    t = time.time()
#    use_itertools(list_1, list_2, n_iter)
#    print("itertools, time needed", time.time()-t)

    t = time.time()
    use_sets(list_1, list_2, n_iter)
    print("set, time needed", time.time()-t)

    t = time.time()
    use_list_comprehension(list_1, list_2, n_iter)
    print("list_comprehension, time needed", time.time()-t)


if __name__ == "__main__":
    main()

# output python3
# itertools, time needed 54.21998310089111
# set, time needed 43.7182936668396
# list_comprehension, time needed 82.10088014602661
#
# output python2
# set, time needed 44.4981598854
# list_comprehension, time needed 336.991075993
