'''
Ensures correctness for _single_filter() using the PyUnit (unittest) package
'''

import unittest # our test package
from parallelogram import helpers # exposes the functions to test

class TestFilter(unittest.TestCase):

	def foo_1(self, elt, index):
		'''
		Filters out odd numbers.
		'''
		return elt % 2 == 0

	def test_filter_1(self):
		'''
		Test a basic filtering case by filtering out odd numbers a small list
		'''
		# ensure correct output when filtering out odd numbers from small list
		output = helpers._single_filter(self.foo_1, range(6))
		self.assertEqual(output, range(0, 6, 2))

	def test_filter_2(self):
		'''
		Test a basic filtering case by filtering out odd numbers of a big list
		'''
		# ensure correct output when filtering out odd numbers from big list
		output = helpers._single_filter(self.foo_1, range(10000))
		self.assertEqual(output, range(0, 10000, 2))	

	def test_filter_3(self):
		'''
		Ensure that filter operates correctly on empty lists
		'''
		# ensure correct output when filtering over empty lists
		output = helpers._single_filter(self.foo_1, [])
		self.assertEqual(output, [])
