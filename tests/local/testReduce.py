'''
Ensures correctness for _single_reduce() using the PyUnit (unittest) package
'''

import unittest # our testing package
from parallelogram import helpers # exposes the functions to test
from nose.tools import assert_raises # so we can test against assertions

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
		Ensure that _single_reduce() assertion fails when an empty list is used.
		'''
		assert_raises(AssertionError, helpers._single_reduce, self.foo_1, [])
