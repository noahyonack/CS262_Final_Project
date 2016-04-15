'''
Ensures correctness for map() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram

class TestMap(unittest.TestCase):

	def test_map(self):
		output = parallelogram.map(1,2)
		self.assertEqual(output, 1)

