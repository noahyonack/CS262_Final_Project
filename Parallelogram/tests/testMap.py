'''
Ensures correctness for map() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram

class TestMap(unittest.TestCase):

	def test_map(self):
		
		def test1(elt, index):
			return elt + 1
		
		output = parallelogram.map([1,2,3], test1)
		self.assertEqual(output, [2,3,4])