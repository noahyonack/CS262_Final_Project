'''
Ensures correctness for map() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram

class TestMap(unittest.TestCase):

	def test_map(self):
		
		def foo(elt):
			return elt + 1
		
		output = parallelogram.map([1,2,3], foo)
		self.assertEqual(output, [2,3,4])