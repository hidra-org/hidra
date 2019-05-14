from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import collections
import time

def use_sets(list_1, list_2, n_iter):
    for i in range(n_iter):
        list(set(list_1)-set(list_2))


def use_list_comprehension(list_1, list_2, n_iter):
    for i in range(n_iter):
        [x for x in list_1 if x not in list_2]


if __name__ == "__main__":

    list_1 = range(100)
    list_2 = collections.deque(maxlen=50)
    for i in range(50, 100):
        list_2.append(i)

    n_iter = 10000000

    t = time.time()
    use_sets(list_1, list_2, n_iter)
    print("set, time needed", time.time()-t)

    t = time.time()
    use_list_comprehension(list_1, list_2, n_iter)
    print("list_comprehension, time needed", time.time()-t)

# output python3
# set, time needed 43.11841416358948
# list_comprehension, time needed 409.5846631526947
#
# set, time needed 43.903398037
# list_comprehension, time needed 472.007302999
