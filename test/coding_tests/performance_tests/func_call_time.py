from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import time

def func():
    pass

def with_func_call(n_iter):
    for i in xrange(n_iter):
        func()

def without_func_call(n_iter):
    for i in xrange(n_iter):
        pass

if __name__ == "__main__":
    n_iter = 1000000000
    print("Testing", n_iter, "iterations")

    t = time.time()
    with_func_call(n_iter)
    print('with func call:', time.time() - t)

    t = time.time()
    without_func_call(n_iter)
    print('without func call:', time.time() - t)
