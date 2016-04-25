'''
Ensures correctness for _single_filter() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import helpers

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
		output_1 = helpers._single_filter(self.foo_1, [1,2,3,4,5,6])
		self.assertEqual(output_1, [2,4,6])

	def test_filter_2(self):
		'''
		Test a basic filtering case by filtering out odd numbers of a big list
		'''
		# ensure correct output when filtering out odd numbers from big list
		output_2 = helpers._single_filter(self.foo_1, range(10000))
		self.assertEqual(output_2, range(0, 10000, 2))	

	def test_filter_3(self):
		'''
		Ensure that filter operates correctly on empty lists
		'''
		# ensure correct output when filtering over empty lists
		output_3 = helpers._single_filter(self.foo_1, [])
		self.assertEqual(output_3, [])



