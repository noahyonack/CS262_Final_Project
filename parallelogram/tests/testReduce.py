'''
Ensures correctness for reduce() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram

class TestReduce(unittest.TestCase):

	def test_reduce(self):
		
		def test1(elt1, elt2):
			return elt1 + elt2
		
		output = parallelogram.p_reduce(test1, [1,2,3])
		self.assertEqual(output, 6)