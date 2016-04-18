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

def p_map(foo, data):
	'''
	Map a function foo() over chunks of data (of type list) and 
	join the mapped chunks before returning back to the caller. 

	This mapping will likely not be done on a single machine (unless the data 
	to be mapped over is so small that sending it over a network would be 
	inefficient.)
	'''
	result = []
	chunks = helpers._chunk_list(data, CHUNK_SIZE)
	for index, chunk in enumerate(chunks):
		mapped_chunk = helpers._single_map(foo, chunk)
		result.extend(mapped_chunk)
		# ideally, we'd like to pop the chunk after processing 
		# it to preserve memory, but this messes up the loop
		# chunks.pop(index)
	return result

def p_filter(foo, data):
	'''
	Filter a function foo() over chunks of data (of type list) and 
	join the filtered chunks before returning back to the caller. 

	This filtering will likely not be done on a single machine (unless the data 
	to be filtered over is so small that sending it over a network would be 
	inefficient.)
	'''
	result = []
	chunks = helpers._chunk_list(data, CHUNK_SIZE)
	for index, chunk in enumerate(chunks):
		filtered_chunk = helpers._single_filter(foo, chunk)
		result.extend(filtered_chunk)
		# ideally, we'd like to pop the chunk after processing 
		# it to preserve memory, but this messes up the loop
		# chunks.pop(index)
	return result

def p_reduce(foo, data):
	'''
	Reduce a function foo() over chunks of data (of type list) and 
	then reduce the results before returning back to the caller. 

	This reduction will likely not be done on a single machine (unless the data 
	to be reduced over is so small that sending it over a network would be 
	inefficient.)

	After the intial chunks have been reduced, we still need to reduce
	the results of the initial reduction, so we call our function again
	and either redistribute the initial results or simply locally-process
	chunks, depending on the size of the initial results array.
	'''
	results = []
	chunks = helpers._chunk_list(data, CHUNK_SIZE)
	for index, chunk in enumerate(chunks):
		reduced_chunk = helpers._single_filter(foo, chunk)
		results.extend(reduced_chunk)
		# ideally, we'd like to pop the chunk after processing 
		# it to preserve memory, but this messes up the loop
		# chunks.pop(index)
	if (len(results) < 1000):
		return helpers._single_reduce(foo, results)
	else:
		return _reduce(foo, data)
