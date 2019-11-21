#!/usr/bin/env python


import threading
import time
import multiprocessing


class TestThread (threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		for i in range(5000):
			file_descriptor = open("/opt/hidra/test_file.tif", "rb")
			data = file_descriptor.read()
			del file_descriptor
			del data


class TestProcess (multiprocessing.Process):
	def __init__(self):
		multiprocessing.Process.__init__(self)

	def run(self):
		for i in range(5000):
			file_descriptor = open("/opt/hidra/test_file.tif", "rb")
			data = file_descriptor.read()
			del file_descriptor
			del data


def main():
	number = 2
	print("number of threads/processes: ", number)

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
