'''
Ensures correctness for _single_map() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import helpers

class TestMap(unittest.TestCase):

	def foo_1(self, elt, index):
		'''
		Increments an element by 1
		'''
		return elt + 1

	def test_map_1(self):
		'''
		Test a basic map case by mapping an incremental function over a
		'''
		# ensure correct output when mapping over a small list
		output = helpers._single_map(self.foo_1, [1,2,3,4,5,6])
		self.assertEqual(output, range(2, 8))
	
	def test_map_2(self):
		'''
		Test a basic map case by mapping an incremental function over a big list
		'''
		# ensure correct output when mapping over a big list
		output = helpers._single_map(self.foo_1, range(10000))
		self.assertEqual(output, range(1, 10001))		

	def test_map_3(self):
		'''
		Ensure that map operates correctly on empty lists.
		'''
		# ensure correct output when mapping over empty lists
		output = helpers._single_map(self.foo_1, [])
		self.assertEqual(output, [])
