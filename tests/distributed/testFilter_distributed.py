'''
Ensures correctness for p_filter() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram
from parallelogram.parallelogram_server import Server
import time

PORT = 1001

class TestFilter_Distributed(unittest.TestCase):
	#setUp and tearDown BROKEN
	# def setUp(self):
	# 	self.server = Server(PORT)
	# 	self.server.start()
	# 	print('started')
    #
	# def tearDown(self):
	# 	self.server.stop()
	# 	print('stopped')

	def test_filter(self):

		def test1(elt, index):
			return elt % 2 == 0

		print('filter')
		output = parallelogram.p_filter(test1, [1,2,3,4,5,6], PORT)
		self.assertEqual(output, [2,4,6])