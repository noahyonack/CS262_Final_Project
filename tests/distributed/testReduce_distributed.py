'''
Ensures correctness for p_reduce() using the PyUnit (unittest) package
'''

import unittest # our testing package
from nose.tools import assert_raises # so we can test against assertions
from parallelogram import parallelogram # library methods 
from parallelogram.config import PORT # PORT on which the server should listen
from parallelogram.parallelogram_server import Server # server api

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

    def foo_1(self, elt1, elt2):
        '''
        Adds two adjacent elements together
        '''
        return elt1 + elt2

    def test_reduce_1(self):
        '''
        Test a basic reduce case by summing a small list
        '''
        output = parallelogram.p_reduce(self.foo_1, range(6), PORT, 10)
        self.assertEqual(output, sum(range(6)))

    def test_reduce_2(self):
        '''
        Test a basic reduce case by summing a big list
        '''
        output = parallelogram.p_reduce(self.foo_1, range(10000), PORT, 10)
        self.assertEqual(output, sum(range(10000)))

    def test_reduce_3(self):
        '''
        Ensure that _single_reduce() assertion fails when an empty 
        list is used.
        '''
        assert_raises(AssertionError, parallelogram.p_reduce, 
            self.foo_1, [], PORT, 10)
