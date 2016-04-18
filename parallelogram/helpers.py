''' 
This file contains helper functions for use in our implementations 
of map(), filter(), and reduce()
'''

def _chunk_list(data, sz):
    '''
    Creates chunks of size `sz` from data. Returns a list of chunks.
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
	for index, elt in enumerate(data):
		data[index] = foo(elt, index)
	return data

def _chunk_map(foo, data):
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