#!/usr/bin/env python

from __future__ import print_function

import threading
import time
import multiprocessing


class TestThread (threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		j = 0.0
		for i in range(100000000):
			j += i * i


class TestProcess (multiprocessing.Process):
	def __init__(self):
		multiprocessing.Process.__init__(self)

	def run(self):
		j = 0.0
		for i in range(100000000):
			j += i * i


def main():
	number = 2
	print("number of threads/processes:", number)

	t = time.time()

	x = {}
	for i in range(number):
		x[i] = TestThread()
		x[i].start()

	for i in range(number):
		x[i].join()

	print("threading:", time.time() - t)

	t = time.time()

	x = {}
	for i in range(number):
		x[i] = TestProcess()
		x[i].start()

	for i in range(number):
		x[i].join()

	print("multiprocessing:", time.time() - t)


if __name__ == "__main__":
	main()

