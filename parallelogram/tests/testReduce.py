'''
Ensures correctness for reduce() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram

class TestReduce(unittest.TestCase):

	def test_reduce(self):
		
		def test1(elt, index):
			return elt + 1
		
		output = parallelogram.reduce([1,2,3], test1)
		self.assertEqual(output, [2,3,4])