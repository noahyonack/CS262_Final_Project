'''
Ensures correctness for p_filter() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram
from parallelogram.parallelogram_server import Server
import time
from parallelogram.config import PORT

class TestFilter_Distributed(unittest.TestCase):
	#setUp and tearDown BROKEN
	# def setUp(self):
	# 	self.server = Server(PORT)
	# 	self.server.start()
	# 	print('started')
 #    #
	# def tearDown(self):
	# 	self.server.stop()
	# 	print('stopped')


	def test_filter_1(self):
		'''
		Test a basic filtering case by filtering out odd numbers a small list
		'''

		def foo_1(elt, index):
			'''
			Filters out odd numbers.
			'''
			return elt % 2 == 0

		# ensure correct output when filtering out odd numbers from small list
		output = parallelogram.p_filter(foo_1, range(6), PORT, 10)
		self.assertEqual(output, range(0, 6, 2))

	def test_filter_2(self):
		'''
		Test a basic filtering case by filtering out odd numbers of a big list
		'''

		def foo_1(elt, index):
			'''
			Filters out odd numbers.
			'''
			return elt % 2 == 0

		# ensure correct output when filtering out odd numbers from big list
		output = parallelogram.p_filter(foo_1, range(100), PORT, 10)
		self.assertEqual(output, range(0, 100, 2))

	def test_filter_3(self):
		'''
		Ensure that filter operates correctly on empty lists
		'''

		def foo_1(elt, index):
			'''
			Filters out odd numbers.
			'''
			return elt % 2 == 0

		# ensure correct output when filtering over empty lists
		output = parallelogram.p_filter(foo_1, [], PORT, 10)
		self.assertEqual(output, [])
