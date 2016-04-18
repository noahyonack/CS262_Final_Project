''' 
This file contains helper functions for use in our parallelized 
implementations  of map(), filter(), and reduce()
'''

def _chunk_list(data, sz):
    '''
    Creates chunks of size `sz` from data. Returns a list of chunks, 
    which are also lists.

    These chunks will be sent to other machines on the network for processing.
    '''
    chunks = []
    for i in xrange(0, len(data), sz):
    	chunk = data[i:i + sz]
    	chunks.append(chunk)
    	# ideally, we could remove the chunked elements from the 
    	# list so we don't clog memory, but this messes up the loop
    	# data[i:i + sz] = []
    return chunks

def _single_map(foo, data):
	'''
	Map a function foo() over data (of type list). Map modifies data in place
	and supplies foo() with both the current element of the list and its
	respective index.

	This function is meant to be used on a chunk, which is a portion of 
	a list designated for a single machine. This function is called by 
	parallelogram.map()
	'''
	for index, elt in enumerate(data):
		data[index] = foo(elt, index)
	return data

def _single_filter(foo, data):
	'''
	Filter data (of type list) via a predicate formatted as a function. For 
	example, if data is a list of natural numbers from 1 to N:

		[1, 2, 3, ..., N]

	And you'd like to filter this list so that it contains only even numbers, 
	then simply define foo() to be:

		def foo(elt, index):
			return elt % 2 == 0

	This function is meant to be used on a chunk, which is a portion of 
	a list designated for a single machine. This function is called by 
	parallelogram.filter()
	'''
	for index, elt in enumerate(data):
		if not foo(elt, index):
			data.pop(index)
	return data