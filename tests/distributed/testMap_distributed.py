'''
Ensures correctness for p_map() using the PyUnit (unittest) package
'''

import unittest
from parallelogram import parallelogram
from parallelogram.parallelogram_server import Server
import time
import numpy as np
# from parallelogram.config import PORT
PORT = 1001

class TestMap_Distributed(unittest.TestCase):
    #setUp and tearDown BROKEN
    # def setUp(self):
    #     self.server = Server(PORT)
    #     self.server.start()
    #     print('started')
    #
    # def tearDown(self):
    #     self.server.stop()
    #     print('stopped')

    def test_map(self):

        def test1(elt, index):
            return elt + 1

        print('map')
        output = parallelogram.p_map(test1, [1,2,3], PORT, 10)
        self.assertEqual(output, [2,3,4])

    #check if cloudpickle can handle libraries that aren't imported
    #success, seems to be able to import and deal with modules it knows
    # def test_cloudpickle_library(self):
    #
    #     def test1(elt, index):
    #         return np.sum(np.arange(elt, 5))
    #
    #     print('map')
    #     output = parallelogram.p_map(test1, [1,2,3], PORT, 10)
    #     self.assertEqual(output, [10,9,7])


    # #check if cloudpickle can handle externally defined classes
    # # IT CAN'T, TEST FAILED, COULDN'T FIND EXTERNAL MODULE
    # def test_cloudpickle_external_class(self):
    #
    #     def test1(elt, index):
    #         a = helpers_test.testClass(elt)
    #         return a.add()
    #
    #     print('map')
    #     output = parallelogram.p_map(test1, [1,2,3], PORT)
    #     self.assertEqual(output, [2,3,4])

    #check if cloudpickle can handle internally defined classes
    # def test_cloudpickle_class(self):
    #
    #     class testClass():
    #         def __init__(self, value):
    #             self.value = value
    #
    #         def add(self):
    #             return(self.value + 1)
    #
    #     def test1(elt, index):
    #         a = testClass(elt)
    #         return a.add()
    #
    #     print('map')
    #     output = parallelogram.p_map(test1, [1,2,3], PORT, 10)
    #     self.assertEqual(output, [2,3,4])