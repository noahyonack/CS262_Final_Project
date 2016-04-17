'''
Ensures correctness for filter() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram

class TestFilter(unittest.TestCase):

	def test_filter(self):
		
		def test1(elt, index):
			return elt % 2 == 0
		
		output = parallelogram.filter([1,2,3,4,5,6], test1)
		self.assertEqual(output, [2,4,6])