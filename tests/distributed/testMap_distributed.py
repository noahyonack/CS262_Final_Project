'''
Ensures correctness for p_map() using the PyUnit (unittest) package
'''

import unittest # our test package
from parallelogram.config import PORT # PORT on which the server should listen
from parallelogram import parallelogram # library methods 
from parallelogram.parallelogram_server import Server # server api

class TestMap_Distributed(unittest.TestCase):

    def test_map_1(self):
        '''
        Test a basic map case by mapping an incremental 
        function over a small list
        '''

        def foo_1(elt, index):
            '''
            Increments an element by 1
            '''
            return elt + 1

        output = parallelogram.p_map(foo_1, [1,2,3,4,5,6], PORT, 10)
        self.assertEqual(output, range(2, 8))
    
    def test_map_2(self):
        '''
        Test a basic map case by mapping an incremental 
        function over a big list
        '''

        def foo_1(elt, index):
            '''
            Increments an element by 1
            '''
            return elt + 1

        output = parallelogram.p_map(foo_1, range(10000), PORT, 30)
        self.assertEqual(output, range(1, 10001))

    def test_map_3(self):
        '''
        Ensure that map operates correctly on empty lists.
        '''
        def foo_1(elt, index):
            '''
            Increments an element by 1
            '''
            return elt + 1

        output = parallelogram.p_map(foo_1, [], PORT, 10)
        self.assertEqual(output, [])
