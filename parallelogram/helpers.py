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