''' 
This file contains our library's implementations of map(), filter(), and 
reduce().

Imports helpers.py, which contains 'private' helper functions
'''

import helpers

'''
Our minimum chunk size in terms of number of elements. When asked to map(),
filter(), or reduce() a presumably large list, we chunk it in pieces of 
CHUNK_SIZE and send those chunks to other physical machines on the network.

If the list on which to apply a function foo() is smaller than CHUNK_SIZE, 
it makes more sense to have the calling machine process the chunk instead
of sending it over the wire.
'''
CHUNK_SIZE = 100

def map(foo, data):
	'''
	Map a function foo() over data (of type list). Map modifies data in place
	and supplies foo() with both the current element of the list and its
	respective index.
	'''
	result = []
	chunks = helpers._chunk_list(data, CHUNK_SIZE)
	for index, chunk in enumerate(chunks):
		mapped_chunk = _single_map(foo, chunk)
		result.extend(mapped_chunk)
		# ideally, we'd like to pop the chunk after processing 
		# it to preserve memory, but this messes up the loop
		# chunks.pop(index)
	return result

def filter(foo, data):
	'''
	Filter data (of type list) via a predicate formatted as a function. For 
	example, if data is a list of natural numbers from 1 to N:

		[1, 2, 3, ..., N]

	And you'd like to filter this list so that it contains only even numbers, 
	then simply define foo() to be:

		def foo(elt, index):
			return elt % 2 == 0
	'''
	for index, elt in enumerate(data):
		if not foo(elt, index):
			data.pop(index)
	return data

def reduce(foo, data):
	'''
	Reduce data (of type list) by continually applying foo() to subsequent
	elements of data. For exmaple, if data is a list of elements e_1 to e_N:

		[e_1, e_2, e_3, ..., e_N]

	And you'd like to reduce this list to the sum of all the elements, then 
	simply define foo() to be:

		def foo(elt1, elt2):
			return elt1 + elt2

	foo() is applied to data in the following way:

		1. [foo(e_1, e_2), e_3, ..., e_N]
		2. [foo(foo(e_1, e_2), e_3), ..., e_N]
		...
		N - 1. [foo(foo(foo(e_1, e_2), e_3), ..., e_N)]
	'''
	# as explained above, we need to apply foo() N-1 times
	for _ in range(len(data) - 1):
		data[0] = foo(data[0], data[1])
		# once the function has been applied, remove the old element
		data.pop(1)
	return data[0]
