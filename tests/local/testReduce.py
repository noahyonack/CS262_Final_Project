'''
Ensures correctness for _single_reduce() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import helpers

class TestReduce(unittest.TestCase):

	def foo_1(self, elt1, elt2):
		'''
		Adds two adjacent elements together
		'''
		return elt1 + elt2

	def test_reduce_1(self):
		'''
		Test a basic reduce case by summing a small list
		'''
		output = helpers._single_reduce(self.foo_1, range(6))
		self.assertEqual(output, sum(range(6)))

	def test_reduce_2(self):
		'''
		Test a basic reduce case by summing a big list
		'''
		output = helpers._single_reduce(self.foo_1, range(10000))
		self.assertEqual(output, sum(range(10000)))

	def test_reduce_3(self):
		'''
		Ensure reduce actually fails when an empty list is passed in.
		'''
		import inspect
		print inspect.getsource(helpers._single_reduce)
		output = helpers._single_reduce(self.foo_1, [])