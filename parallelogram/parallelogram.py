''' 
This file contains our library's implementations of p_map(), p_filter(), and 
p_reduce().

We use the letter "p" because it indicates that the method is paralellized
and because doing so ensures that our functions are properly namespaced

Imports helpers.py, which contains 'private' helper functions
'''

import helpers
import threading
import itertools

'''
Our minimum chunk size in terms of number of elements. When asked to map(),
filter(), or reduce() a presumably large list, we chunk it in pieces of 
CHUNK_SIZE and send those chunks to other physical machines on the network.

If the list on which to apply a function foo() is smaller than CHUNK_SIZE, 
it makes more sense to have the calling machine process the chunk instead
of sending it over the wire.
'''
CHUNK_SIZE = 6

def p_map(foo, data, port):
    '''
    Map a function foo() over chunks of data (of type list) and
    join the mapped chunks before returning back to the caller.

    This mapping will likely not be done on a single machine (unless the data
    to be mapped over is so small that sending it over a network would be
    inefficient.)

    :param foo:
    :param data:
    :return:
    '''
    chunks = helpers._chunk_list(data, CHUNK_SIZE)
    #data read into this list
    result = [None] * len(chunks)
    #list of threads corresponding to sent chunks
    compute_threads = [None] * len(chunks)
    for index, chunk in enumerate(chunks):
        #spawns separate thread to distribute each chunk and collect results
        compute_threads[index] = threading.Thread(
            target = helpers._send_op, 
            args = (result, foo, chunk, 'map', index, port))
        compute_threads[index].start()
		# ideally, we'd like to pop the chunk after processing
		# it to preserve memory, but this messes up the loop
		# chunks.pop(index)
    #wait for all threads to finish, and thus for all results to come in
    for thread in compute_threads:
        thread.join()
    return flatten(result)

def p_filter(foo, data, port):
    '''
    Filter a function foo() over chunks of data (of type list) and
    join the filtered chunks before returning back to the caller.

	This filtering will likely not be done on a single machine (unless the data
	to be filtered over is so small that sending it over a network would be
	inefficient.)

	:param foo:
	:param data:
	:return:
	'''
    chunks = helpers._chunk_list(data, CHUNK_SIZE)
    result = [None] * len(chunks) #data read into this list
    #list of threads corresponding to sent chunks
    compute_threads = [None] * len(chunks)
    for index, chunk in enumerate(chunks):
        #spawns separate thread to distribute each chunk and collect results
        compute_threads[index] = threading.Thread(
            target = helpers._send_op, 
            args = (result, foo, chunk, 'filter', index, port))
        compute_threads[index].start()
		# ideally, we'd like to pop the chunk after processing
		# it to preserve memory, but this messes up the loop
		# chunks.pop(index)
    #wait for all threads to finish, and thus for all results to come in
    for thread in compute_threads:
        thread.join()
    return flatten(result)

def p_reduce(foo, data, port):
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
    
    :param foo:
    :param data:
    :return:
    '''
    chunks = helpers._chunk_list(data, CHUNK_SIZE)
    result = [None] * len(chunks) #data read into this list
    #list of threads corresponding to sent chunks
    compute_threads = [None] * len(chunks)
    for index, chunk in enumerate(chunks):
        #spawns separate thread to distribute each chunk and collect results
        compute_threads[index] = threading.Thread(
            target = helpers._send_op, 
            args = (result, foo, chunk, 'reduce', index, port))
        compute_threads[index].start()
		# ideally, we'd like to pop the chunk after processing
		# it to preserve memory, but this messes up the loop
		# chunks.pop(index)
    #wait for all threads to finish, and thus for all results to come in
    for thread in compute_threads:
        thread.join()
    #checks if single value is returned, as then reduce is done
    if (len(result) == 1):
        return result[0]
    else:
        result = flatten(result)
        if (len(result) <= CHUNK_SIZE):
            return helpers._single_reduce(foo, result)
        else:
            return p_reduce(foo, result)
