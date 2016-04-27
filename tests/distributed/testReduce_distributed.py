'''
Ensures correctness for p_reduce() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram
from parallelogram.parallelogram_server import Server
import time
from config import PORT

class TestReduce_Distributed(unittest.TestCase):
    #setUp and tearDown BROKEN
    # def setUp(self):
    #     self.server = Server(PORT)
    #     self.server.start()
    #     print('started')
    #
    # def tearDown(self):
    #     self.server.stop()
    #     print('stopped')

    def test_reduce(self):

        def test1(elt1, elt2):
            return elt1 + elt2

        print('reduce')
        output = parallelogram.p_reduce(test1, [1,2,3,4,5,6], PORT)
        self.assertEqual(output, 21)