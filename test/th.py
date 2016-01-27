#!/usr/bin/env python

import time

import threading
import multiprocessing


class th (threading.Thread):
	def __init__ (self):
		threading.Thread.__init__(self)

	def run (self):
		j = 0.0
		for i in xrange(100000000):
			j += i * i

class pr (multiprocessing.Process):
	def __init__ (self):
		multiprocessing.Process.__init__(self)

	def run (self):
		j = 0.0
		for i in xrange(100000000):
			j += i * i

number = 2

print 'number of threads/processes: ', number

t = time.time()

x = {}

for i in xrange(number):
	x[i] = th()
	x[i].start()

for i in xrange(number):
	x[i].join()

print 'threading:', time.time() - t

t = time.time()

x = {}

for i in xrange(number):
	x[i] = pr()
	x[i].start()

for i in xrange(number):
	x[i].join()

print 'multiprocessing:', time.time() - t