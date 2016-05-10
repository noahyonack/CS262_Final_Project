'''
Ensures correctness for p_filter() using the PyUnit (unittest) package
'''

import unittest # our test package
from parallelogram.config import PORT # PORT on which the server should listen
from parallelogram import parallelogram # library methods 
from parallelogram.parallelogram_server import Server # server api

class TestFilter_Distributed(unittest.TestCase):

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
		output = parallelogram.p_filter(foo_1, range(1000), PORT, 10)
		self.assertEqual(output, range(0, 1000, 2))

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
