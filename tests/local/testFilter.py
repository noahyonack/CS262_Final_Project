'''
Ensures correctness for _single_filter() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import helpers

class TestFilter(unittest.TestCase):

	def test_filter_1(self):
		
		def test1(elt, index):
			return elt % 2 == 0

		output_1 = helpers._single_filter(test1, [1,2,3,4,5,6])
		self.assertEqual(output_1, [2,4,6])

		output_2 = helpers._single_filter(test1, [])
		self.assertEqual(output_2, [])
		

	def test_filter_2(self):

		def test2(elt, index):
			return elt == -1 

		big_list = range(10000)
		output_3 = helpers._single_filter(test2, big_list)
		self.assertEqual(output_3, [])